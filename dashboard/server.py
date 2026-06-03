from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .health import HEARTBEAT_MAX_AGE_SECONDS
from .schemas import BotLog, Heartbeat, MarketState, Signal, SignalKindGroupStats, Trade, WatchlistItem
from .signal_outcomes import SignalOutcomeStore, refresh_signal_outcomes
from .store import DashboardStore
from orderflow_accum.signal_taxonomy import HIGH_POTENTIAL_KINDS, normalize_signal_kind, signal_family, signal_focus_group

STATIC_DIR = Path(__file__).resolve().parent / "static"
SIGNALS_DB_PATH = Path("data/signals.db")


class WebSocketHub:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._clients.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(websocket)

    async def broadcast(self, event: str, payload: object) -> None:
        message = json.dumps({"event": event, "payload": _jsonable(payload)}, ensure_ascii=False)
        async with self._lock:
            clients = list(self._clients)
        for client in clients:
            try:
                await client.send_text(message)
            except RuntimeError:
                await self.disconnect(client)


def _dashboard_ingest_token() -> str:
    return os.getenv("DASHBOARD_INGEST_TOKEN", "").strip()


def verify_ingest_auth(authorization: str | None = Header(default=None)) -> None:
    token = _dashboard_ingest_token()
    if not token:
        return
    expected = f"Bearer {token}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Missing or invalid ingest bearer token")


def _jsonable(payload: object) -> object:
    return jsonable_encoder(payload)


async def _live_refresh_loop(store: DashboardStore, hub: WebSocketHub) -> None:
    while True:
        try:
            await store.refresh_live_data()
            await hub.broadcast("snapshot", await store.snapshot())
        except Exception as exc:
            await store.add_log(BotLog(message=f"Live refresh failed: {exc}", source="dashboard", severity="error"))
            await hub.broadcast("snapshot", await store.snapshot())
        await asyncio.sleep(60)


def _parse_db_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    candidates = [text]
    if "+" not in text and "T" in text:
        candidates.append(f"{text}+00:00")
    if "+" not in text and " " in text:
        candidates.append(f"{text.replace(' ', 'T')}+00:00")
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _latest_executor_event_at(conn: sqlite3.Connection) -> datetime | None:
    columns = _table_columns(conn, "trade_lifecycle_events")
    if not {"event_type", "created_at"}.issubset(columns):
        return None
    row = conn.execute(
        """
        SELECT created_at
        FROM trade_lifecycle_events
        WHERE event_type LIKE 'EXECUTOR%'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    return _parse_db_timestamp(row["created_at"] if row else None)


def _latest_executor_outcome_at(conn: sqlite3.Connection) -> datetime | None:
    columns = _table_columns(conn, "executor_outcomes")
    if "updated_at" not in columns:
        return None
    row = conn.execute("SELECT updated_at FROM executor_outcomes ORDER BY updated_at DESC LIMIT 1").fetchone()
    return _parse_db_timestamp(row["updated_at"] if row else None)


def _executor_open_trades(conn: sqlite3.Connection) -> int:
    columns = _table_columns(conn, "executor_outcomes")
    if not {"state", "action"}.issubset(columns):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM executor_outcomes
        WHERE UPPER(COALESCE(state, '')) != 'EXITED'
          AND (
              UPPER(COALESCE(state, '')) IN ('ENTERED', 'PROTECT_BREAKEVEN')
              OR UPPER(COALESCE(action, '')) = 'HOLD'
          )
        """
    ).fetchone()
    return int(row["total"] if row else 0)


def _executor_closed_trades_today(conn: sqlite3.Connection) -> int:
    columns = _table_columns(conn, "executor_trades")
    if "exit_time" not in columns:
        return 0
    today = datetime.now(timezone.utc).date().isoformat()
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM executor_trades
        WHERE exit_time IS NOT NULL
          AND date(exit_time) = ?
        """,
        (today,),
    ).fetchone()
    return int(row["total"] if row else 0)


# executor online status is based on the freshest heartbeat or executor activity timestamp.
def _executor_status_from_activity(heartbeat: Heartbeat | None, latest_activity_at: datetime | None) -> str:
    timestamps: list[datetime] = []
    if heartbeat is not None:
        timestamps.append(heartbeat.timestamp.astimezone(timezone.utc))
    if latest_activity_at is not None:
        timestamps.append(latest_activity_at)
    if not timestamps:
        return "no-heartbeat"
    latest = max(timestamps)
    age_seconds = (datetime.now(timezone.utc) - latest).total_seconds()
    return "online" if age_seconds <= HEARTBEAT_MAX_AGE_SECONDS else "stale"


def _executor_status_fields(heartbeats: dict[str, Heartbeat]) -> dict[str, int | str]:
    fields: dict[str, int | str] = {
        "executor": _executor_status_from_activity(heartbeats.get("executor"), None),
        "open_trades": 0,
        "closed_trades_today": 0,
    }
    if not SIGNALS_DB_PATH.exists():
        return fields

    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        latest_activity_at = max(
            (timestamp for timestamp in (_latest_executor_event_at(conn), _latest_executor_outcome_at(conn)) if timestamp is not None),
            default=None,
        )
        fields["executor"] = _executor_status_from_activity(heartbeats.get("executor"), latest_activity_at)
        fields["open_trades"] = _executor_open_trades(conn)
        fields["closed_trades_today"] = _executor_closed_trades_today(conn)
        return fields
    finally:
        conn.close()


def _signal_kind_group_empty() -> dict[str, float | int]:
    return {
        "total": 0,
        "tp2": 0,
        "sl": 0,
        "expired": 0,
        "confirmed": 0,
        "score_last_sum": 0.0,
        "score_max_sum": 0.0,
        "max_gain_sum": 0.0,
        "max_drawdown_sum": 0.0,
    }


def _status_or_outcome(row: sqlite3.Row) -> str:
    keys = set(row.keys())
    outcome = str(row["outcome"] or "").strip().upper() if "outcome" in keys else ""
    status = str(row["status"] or "").strip().upper() if "status" in keys else ""
    return outcome or status


def _is_confirmed_signal(row: sqlite3.Row) -> bool:
    status = str(row["status"] or "").strip().upper() if "status" in row.keys() else ""
    return status in {"CONFIRMED", "CONFIRMED_LONG", "CONFIRMED_SHORT"}


def _management_recommendation(
    *,
    focus_group: str,
    total: int,
    tp2: int,
    sl: int,
    expired: int,
    tp2_rate_closed_pct: float,
    avg_max_gain_pct: float,
) -> str:
    if focus_group == "EXECUTION_STABLE":
        return "normal_tp_sl"
    if focus_group == "EXPERIMENTAL":
        return "paper_only_low_priority"
    if focus_group != "HIGH_POTENTIAL":
        return "monitor"

    expired_share = (expired / total) if total else 0.0
    if expired_share >= 0.4:
        return "extend_watch_window_or_wait_for_confirmation"
    if avg_max_gain_pct >= 3.0 and tp2_rate_closed_pct < 50.0:
        return "breakeven_first_trailing_candidate"
    if tp2_rate_closed_pct >= 50.0 and tp2 >= max(sl, 1):
        return "priority_high_potential"
    return "monitor_high_potential"


def _finalize_signal_kind_groups(groups: dict[tuple[str, str, str, str, str], dict[str, float | int]]) -> list[SignalKindGroupStats]:
    rows: list[SignalKindGroupStats] = []
    for (kind, family, focus_group, timeframe, source), metrics in groups.items():
        total = int(metrics["total"])
        closed_total = int(metrics["tp2"]) + int(metrics["sl"]) + int(metrics["expired"])
        rows.append(
            SignalKindGroupStats(
                kind=kind,
                signal_family=family,
                signal_focus_group=focus_group,
                timeframe=timeframe,
                source=source,
                total=total,
                tp2=int(metrics["tp2"]),
                sl=int(metrics["sl"]),
                expired=int(metrics["expired"]),
                confirmed=int(metrics["confirmed"]),
                tp2_rate_closed_pct=round((int(metrics["tp2"]) / closed_total) * 100.0, 2) if closed_total else 0.0,
                avg_score_last=round(float(metrics["score_last_sum"]) / total, 4) if total else 0.0,
                avg_score_max=round(float(metrics["score_max_sum"]) / total, 4) if total else 0.0,
                avg_max_gain_pct=round(float(metrics["max_gain_sum"]) / total, 4) if total else 0.0,
                avg_max_drawdown_pct=round(float(metrics["max_drawdown_sum"]) / total, 4) if total else 0.0,
            )
        )
        rows[-1].recommended_management = _management_recommendation(
            focus_group=focus_group,
            total=total,
            tp2=int(metrics["tp2"]),
            sl=int(metrics["sl"]),
            expired=int(metrics["expired"]),
            tp2_rate_closed_pct=rows[-1].tp2_rate_closed_pct,
            avg_max_gain_pct=rows[-1].avg_max_gain_pct,
        )
    return sorted(rows, key=lambda row: (row.signal_focus_group, row.kind, row.timeframe, row.source))


def _empty_high_potential_focus() -> dict[str, object]:
    return {
        "high_potential_summary": [],
        "by_kind": [],
        "by_timeframe": [],
        "by_symbol": [],
        "by_kind_timeframe": [],
        "management_recommendations": [],
        "focus_group_comparison": [],
    }


def _aggregate_focus_rows(rows: list[sqlite3.Row], group_fields: tuple[str, ...], *, high_potential_only: bool) -> list[dict[str, object]]:
    grouped: dict[tuple[str, ...], dict[str, object]] = {}
    for row in rows:
        kind = normalize_signal_kind(row["kind"]) or "UNKNOWN"
        family = signal_family(kind)
        focus_group = signal_focus_group(kind)
        if high_potential_only and focus_group != "HIGH_POTENTIAL":
            continue
        values = {
            "signal_focus_group": focus_group,
            "signal_family": family,
            "kind": kind,
            "timeframe": str(row["timeframe"] or "UNKNOWN"),
            "symbol": str(row["symbol"] or "UNKNOWN"),
        }
        key = tuple(str(values[field]) for field in group_fields)
        metrics = grouped.setdefault(
            key,
            {
                **{field: values[field] for field in group_fields},
                "signal_focus_group": focus_group,
                "signal_family": family if "kind" in group_fields else "MIXED",
                **_signal_kind_group_empty(),
            },
        )
        metrics["total"] = int(metrics["total"]) + 1
        metrics["score_last_sum"] = float(metrics["score_last_sum"]) + float(row["score_last"] or 0.0)
        metrics["score_max_sum"] = float(metrics["score_max_sum"]) + float(row["score_max"] or 0.0)
        metrics["max_gain_sum"] = float(metrics["max_gain_sum"]) + float(row["max_gain_pct"] or 0.0)
        metrics["max_drawdown_sum"] = float(metrics["max_drawdown_sum"]) + float(row["max_drawdown_pct"] or 0.0)

        result = _status_or_outcome(row)
        if result == "TP2":
            metrics["tp2"] = int(metrics["tp2"]) + 1
        elif result == "SL":
            metrics["sl"] = int(metrics["sl"]) + 1
        elif result == "EXPIRED":
            metrics["expired"] = int(metrics["expired"]) + 1
        if _is_confirmed_signal(row):
            metrics["confirmed"] = int(metrics["confirmed"]) + 1

    result_rows: list[dict[str, object]] = []
    for metrics in grouped.values():
        total = int(metrics["total"])
        closed_total = int(metrics["tp2"]) + int(metrics["sl"]) + int(metrics["expired"])
        tp2_rate = round((int(metrics["tp2"]) / closed_total) * 100.0, 2) if closed_total else 0.0
        avg_gain = round(float(metrics["max_gain_sum"]) / total, 4) if total else 0.0
        output = {
            field: metrics[field]
            for field in group_fields
        }
        output.update(
            {
                "signal_focus_group": metrics["signal_focus_group"],
                "signal_family": metrics["signal_family"],
                "total": total,
                "tp2": int(metrics["tp2"]),
                "sl": int(metrics["sl"]),
                "expired": int(metrics["expired"]),
                "confirmed": int(metrics["confirmed"]),
                "tp2_rate_closed_pct": tp2_rate,
                "avg_score_last": round(float(metrics["score_last_sum"]) / total, 4) if total else 0.0,
                "avg_score_max": round(float(metrics["score_max_sum"]) / total, 4) if total else 0.0,
                "avg_max_gain_pct": avg_gain,
                "avg_max_drawdown_pct": round(float(metrics["max_drawdown_sum"]) / total, 4) if total else 0.0,
                "recommended_management": _management_recommendation(
                    focus_group=str(metrics["signal_focus_group"]),
                    total=total,
                    tp2=int(metrics["tp2"]),
                    sl=int(metrics["sl"]),
                    expired=int(metrics["expired"]),
                    tp2_rate_closed_pct=tp2_rate,
                    avg_max_gain_pct=avg_gain,
                ),
            }
        )
        result_rows.append(output)
    return sorted(result_rows, key=lambda row: tuple(str(row.get(field, "")) for field in group_fields))


def _high_potential_focus_payload(rows: list[sqlite3.Row]) -> dict[str, object]:
    by_kind = _aggregate_focus_rows(rows, ("kind",), high_potential_only=True)
    present = {str(row.get("kind")) for row in by_kind}
    for kind in sorted(HIGH_POTENTIAL_KINDS - present):
        by_kind.append(
            {
                "kind": kind,
                "signal_family": signal_family(kind),
                "signal_focus_group": "HIGH_POTENTIAL",
                "total": 0,
                "tp2": 0,
                "sl": 0,
                "expired": 0,
                "confirmed": 0,
                "tp2_rate_closed_pct": 0.0,
                "avg_score_last": 0.0,
                "avg_score_max": 0.0,
                "avg_max_gain_pct": 0.0,
                "avg_max_drawdown_pct": 0.0,
                "recommended_management": "monitor_high_potential",
            }
        )
    priority = ["ACCUMULATION_WATCH", "ABSORPTION_ZONE", "PRE_IMPULSE_ZONE"]
    by_kind = sorted(by_kind, key=lambda row: priority.index(str(row["kind"])) if str(row["kind"]) in priority else 99)
    recommendations = Counter(str(row["recommended_management"]) for row in by_kind if int(row.get("total", 0)) > 0)
    return {
        "high_potential_summary": _aggregate_focus_rows(rows, ("signal_focus_group",), high_potential_only=True),
        "by_kind": by_kind,
        "by_timeframe": _aggregate_focus_rows(rows, ("timeframe",), high_potential_only=True),
        "by_symbol": _aggregate_focus_rows(rows, ("symbol",), high_potential_only=True),
        "by_kind_timeframe": _aggregate_focus_rows(rows, ("kind", "timeframe"), high_potential_only=True),
        "management_recommendations": [
            {"recommendation": recommendation, "total_groups": total}
            for recommendation, total in sorted(recommendations.items())
        ],
        "focus_group_comparison": _aggregate_focus_rows(rows, ("signal_focus_group",), high_potential_only=False),
    }


def _read_signal_metric_rows() -> list[sqlite3.Row]:
    if not SIGNALS_DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        required = {"kind", "timeframe", "source", "symbol", "status", "score_last", "score_max", "max_gain_pct", "max_drawdown_pct"}
        if not required.issubset(columns):
            return []
        optional_columns = [name for name in ("outcome",) if name in columns]
        select_columns = ["kind", "timeframe", "source", "symbol", "status", "score_last", "score_max", "max_gain_pct", "max_drawdown_pct", *optional_columns]
        return conn.execute(f"SELECT {', '.join(select_columns)} FROM signals").fetchall()
    finally:
        conn.close()


def create_app() -> FastAPI:
    store = DashboardStore()
    hub = WebSocketHub()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.refresh_task = asyncio.create_task(_live_refresh_loop(store, hub))
        try:
            yield
        finally:
            task = app.state.refresh_task
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(
        title="CandleVision Dashboard API",
        version="0.1.0",
        description="MVP API for bot console, market state, signals, dominance strips, watchlist, trades and coin analytics.",
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.hub = hub

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/status")
    async def status():
        snapshot = await store.snapshot()
        current_status = deepcopy(snapshot.status)
        executor_fields = _executor_status_fields(snapshot.heartbeats)
        current_status.executor = str(executor_fields["executor"])
        current_status.open_trades = int(executor_fields["open_trades"])
        current_status.closed_trades_today = int(executor_fields["closed_trades_today"])
        return current_status

    @app.get("/api/market-state")
    async def market_state():
        return (await store.snapshot()).market_state

    @app.get("/api/dominance")
    async def dominance():
        return (await store.snapshot()).pressure_strips

    @app.get("/api/logs")
    async def logs(limit: Annotated[int, Query(ge=1, le=500)] = 120):
        return (await store.snapshot()).logs[:limit]

    @app.get("/api/signals")
    async def signals(
        strength: str | None = None,
        signal_type: str | None = None,
        exchange: str | None = None,
        timeframe: str | None = None,
    ):
        return await store.list_signals(strength, signal_type, exchange, timeframe)

    @app.get("/api/active-setups")
    async def active_setups(limit: Annotated[int, Query(ge=1, le=2000)] = 500):
        if not SIGNALS_DB_PATH.exists():
            return []
        statuses = ("WATCHING", "ACCUMULATION", "PRE_IMPULSE", "BREAKOUT_PRESSURE", "PENDING")
        query = f"""
            SELECT
                id,
                signal_key,
                symbol,
                market,
                timeframe,
                source,
                kind,
                side,
                score_first,
                score_last,
                score_max,
                entry,
                stop_loss,
                take_profit_1,
                take_profit_2,
                first_seen,
                last_seen,
                repeat_count,
                status,
                reasons_last,
                max_gain_pct,
                max_drawdown_pct
            FROM signals
            WHERE status IN ({",".join("?" for _ in statuses)})
            ORDER BY last_seen DESC
            LIMIT ?
        """
        conn = sqlite3.connect(str(SIGNALS_DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(query, (*statuses, limit)).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()


    @app.get("/api/signal-kind-groups")
    async def signal_kind_groups():
        if not SIGNALS_DB_PATH.exists():
            return {"groups": [], "focus_groups": {group: [] for group in ("HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER")}}

        rows = _read_signal_metric_rows()
        grouped: dict[tuple[str, str, str, str, str], dict[str, float | int]] = defaultdict(_signal_kind_group_empty)
        for row in rows:
            kind = normalize_signal_kind(row["kind"]) or "UNKNOWN"
            family = signal_family(kind)
            focus_group = signal_focus_group(kind)
            timeframe = str(row["timeframe"] or "UNKNOWN")
            source = str(row["source"] or "UNKNOWN")
            key = (kind, family, focus_group, timeframe, source)
            metrics = grouped[key]
            metrics["total"] = int(metrics["total"]) + 1
            metrics["score_last_sum"] = float(metrics["score_last_sum"]) + float(row["score_last"] or 0.0)
            metrics["score_max_sum"] = float(metrics["score_max_sum"]) + float(row["score_max"] or 0.0)
            metrics["max_gain_sum"] = float(metrics["max_gain_sum"]) + float(row["max_gain_pct"] or 0.0)
            metrics["max_drawdown_sum"] = float(metrics["max_drawdown_sum"]) + float(row["max_drawdown_pct"] or 0.0)

            result = _status_or_outcome(row)
            if result == "TP2":
                metrics["tp2"] = int(metrics["tp2"]) + 1
            elif result == "SL":
                metrics["sl"] = int(metrics["sl"]) + 1
            elif result == "EXPIRED":
                metrics["expired"] = int(metrics["expired"]) + 1
            if _is_confirmed_signal(row):
                metrics["confirmed"] = int(metrics["confirmed"]) + 1

        groups = _finalize_signal_kind_groups(grouped)
        focus_groups = {focus_group: [] for focus_group in ("HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER")}
        for row in groups:
            focus_groups.setdefault(row.signal_focus_group, []).append(row)
        return {"groups": groups, "focus_groups": focus_groups, "high_potential_focus": _high_potential_focus_payload(rows)}

    @app.get("/api/high-potential-focus")
    async def high_potential_focus():
        if not SIGNALS_DB_PATH.exists():
            return _empty_high_potential_focus()
        return _high_potential_focus_payload(_read_signal_metric_rows())

    @app.get("/api/setup-performance")
    async def setup_performance():
        if not SIGNALS_DB_PATH.exists():
            return {"by_reason": [], "by_score_bucket": [], "by_timeframe": [], "by_kind": [], "by_source": [], "by_family": [], "by_focus_group": []}

        conn = sqlite3.connect(str(SIGNALS_DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT reasons_last, score_last, timeframe, kind, source, status, max_gain_pct, max_drawdown_pct FROM signals"
            ).fetchall()
        finally:
            conn.close()

        def bucket(score: float) -> str:
            if score < 5:
                return "<5"
            if score < 7:
                return "5-7"
            if score < 9:
                return "7-9"
            if score < 11:
                return "9-11"
            return "11+"

        def wl(status: str) -> str:
            s = (status or "").upper()
            if s in {"TP1", "TP2"}:
                return "TP"
            if s == "SL":
                return "SL"
            return "OTHER"

        reason_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "mfe": 0.0, "mae": 0.0})
        score_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
        tf_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
        kind_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
        source_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
        family_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
        focus_stats = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})

        for row in rows:
            outcome = wl(str(row["status"] or ""))
            score = float(row["score_last"] or 0.0)
            tf = str(row["timeframe"] or "1")
            kind = str(row["kind"] or "UNKNOWN") if "kind" in row.keys() else "UNKNOWN"
            source = str(row["source"] or "UNKNOWN") if "source" in row.keys() else "UNKNOWN"
            family = signal_family(kind)
            focus_group = signal_focus_group(kind)
            score_b = bucket(score)
            mfe = float(row["max_gain_pct"] or 0.0)
            mae = float(row["max_drawdown_pct"] or 0.0)
            try:
                reasons = json.loads(row["reasons_last"] or "[]")
                if not isinstance(reasons, list):
                    reasons = []
            except Exception:
                reasons = []

            for reason in reasons:
                entry = reason_stats[str(reason)]
                entry["total"] += 1
                entry["mfe"] += mfe
                entry["mae"] += mae
                if outcome == "TP":
                    entry["tp"] += 1
                elif outcome == "SL":
                    entry["sl"] += 1

            grouped_buckets = (
                score_stats[score_b],
                tf_stats[tf],
                kind_stats[kind],
                source_stats[source],
                family_stats[family],
                focus_stats[focus_group],
            )
            for group in grouped_buckets:
                group["total"] += 1
                group["mfe"] += mfe
                group["mae"] += mae
                if outcome == "TP":
                    group["tp"] += 1
                elif outcome == "SL":
                    group["sl"] += 1
                else:
                    group["pending"] += 1

        def finalize(items: dict, label: str, allowed_labels: tuple[str, ...] | None = None) -> list[dict]:
            out = []
            source_items = dict(items)
            if allowed_labels is not None:
                empty_metrics = {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0}
                source_items = {
                    label_value: source_items.get(label_value, empty_metrics.copy())
                    for label_value in allowed_labels
                }
            for key, value in source_items.items():
                total = max(int(value["total"]), 1)
                tp = int(value["tp"])
                sl = int(value["sl"])
                win_rate = (tp / max(tp + sl, 1)) * 100.0
                out.append(
                    {
                        label: key,
                        "total": int(value["total"]),
                        "tp": tp,
                        "sl": sl,
                        "pending": int(value.get("pending", 0)),
                        "win_rate": round(win_rate, 2),
                        "avg_mfe": round(value["mfe"] / total, 4),
                        "avg_mae": round(value["mae"] / total, 4),
                    }
                )
            if allowed_labels is not None:
                priority = {label_value: index for index, label_value in enumerate(allowed_labels)}
                return sorted(out, key=lambda row: priority.get(str(row[label]), len(priority)))
            return sorted(out, key=lambda row: row["total"], reverse=True)

        focus_taxonomy_labels = ("HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER")
        family_taxonomy_labels = (
            "HIGH_POTENTIAL_ACCUMULATION",
            "HIGH_POTENTIAL_ABSORPTION",
            "HIGH_POTENTIAL_PRE_IMPULSE",
            "EXECUTION_STABLE_BREAKOUT",
            "EXPERIMENTAL_EARLY",
            "EXPERIMENTAL_READY",
            "EXPERIMENTAL_BASE_BUILDUP",
            "OTHER",
        )

        return {
            "by_reason": finalize(reason_stats, "reason"),
            "by_score_bucket": finalize(score_stats, "score_bucket"),
            "by_timeframe": finalize(tf_stats, "timeframe"),
            "by_kind": finalize(kind_stats, "kind"),
            "by_source": finalize(source_stats, "source"),
            "by_family": finalize(family_stats, "family", family_taxonomy_labels),
            "by_focus_group": finalize(focus_stats, "focus_group", focus_taxonomy_labels),
        }

    @app.get("/api/watchlist")
    async def watchlist():
        return (await store.snapshot()).watchlist

    @app.get("/api/trades")
    async def trades():
        return (await store.snapshot()).trades

    @app.get("/api/health")
    async def health():
        snapshot = await store.snapshot()
        return {"status": snapshot.status, "heartbeats": snapshot.heartbeats}

    @app.get("/api/coin/{symbol}")
    async def coin(symbol: str):
        try:
            return await store.coin(symbol)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Live coin data unavailable: {exc}") from exc

    @app.post("/api/refresh")
    async def refresh():
        await store.refresh_live_data()
        snapshot = await store.snapshot()
        await hub.broadcast("snapshot", snapshot)
        return snapshot

    @app.get("/api/snapshot")
    async def snapshot():
        return await store.snapshot()

    @app.post("/api/ingest/log")
    async def ingest_log(log: BotLog, _: None = Depends(verify_ingest_auth)):
        saved = await store.add_log(log)
        await hub.broadcast("log", saved)
        return saved

    @app.post("/api/ingest/signal")
    async def ingest_signal(signal: Signal, _: None = Depends(verify_ingest_auth)):
        saved = await store.add_signal(signal)
        await hub.broadcast("signal", saved)
        return saved

    @app.post("/api/ingest/watchlist")
    async def ingest_watchlist(item: WatchlistItem, _: None = Depends(verify_ingest_auth)):
        saved = await store.add_watchlist_item(item)
        await hub.broadcast("watchlist", saved)
        return saved

    @app.post("/api/ingest/trade")
    async def ingest_trade(trade: Trade, _: None = Depends(verify_ingest_auth)):
        saved = await store.add_trade(trade)
        await hub.broadcast("trade", saved)
        return saved

    @app.post("/api/ingest/heartbeat")
    async def ingest_heartbeat(heartbeat: Heartbeat, _: None = Depends(verify_ingest_auth)):
        saved = await store.add_heartbeat(heartbeat)
        await hub.broadcast("heartbeat", saved)
        return saved

    @app.post("/api/ingest/market-state")
    async def ingest_market_state(state: MarketState, _: None = Depends(verify_ingest_auth)):
        saved = await store.update_market_state(state)
        await hub.broadcast("market-state", saved)
        return saved


    @app.get("/api/signal-outcomes")
    async def signal_outcomes(limit: Annotated[int, Query(ge=1, le=1000)] = 500):
        return SignalOutcomeStore().list_outcomes(limit=limit)

    @app.get("/api/signal-stats")
    async def signal_stats():
        return SignalOutcomeStore().stats()

    @app.post("/api/signal-outcomes/refresh")
    async def refresh_signal_stats(_: None = Depends(verify_ingest_auth)):
        snapshot = await store.snapshot()
        outcomes = await refresh_signal_outcomes(snapshot.signals)
        stats = SignalOutcomeStore().stats()
        await hub.broadcast("signal-stats", stats)
        return {"refreshed": len(outcomes), "stats": stats}

    @app.websocket("/ws")
    async def websocket(websocket: WebSocket):
        await hub.connect(websocket)
        try:
            await websocket.send_json(_jsonable(await store.snapshot()))
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            await hub.disconnect(websocket)

    return app


app = create_app()
