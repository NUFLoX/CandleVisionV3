from __future__ import annotations

import asyncio
import contextlib
import csv
import json
import os
import sqlite3
from collections import Counter, defaultdict
from statistics import median
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
from .schemas import (
    ActiveExecutorTrade,
    ActiveExecutorTradesResponse,
    ActiveExecutorTradesSummary,
    BotLog,
    Heartbeat,
    MarketState,
    Signal,
    SignalKindGroupStats,
    Trade,
    WatchlistItem,
)
from .signal_outcomes import SignalOutcomeStore, refresh_signal_outcomes
from .store import DashboardStore
from orderflow_accum.signal_taxonomy import HIGH_POTENTIAL_KINDS, normalize_signal_kind, signal_family, signal_focus_group

STATIC_DIR = Path(__file__).resolve().parent / "static"
SIGNALS_DB_PATH = Path("data/signals.db")
PROFIT_BACKTEST_DIR = Path("reports_profit_backtest")
PROFIT_BY_KIND_REPORT = "signal_profit_by_kind.csv"
PROFIT_SUMMARY_REPORT = "signal_profit_summary.json"
PROFIT_POTENTIAL_KINDS = ("ACCUMULATION_WATCH", "ABSORPTION_ZONE", "PRE_IMPULSE_ZONE", "BREAKOUT_PRESSURE")
PROFIT_POTENTIAL_METRICS = (
    "avg_max_gain_pct",
    "median_max_gain_pct",
    "max_gain_pct",
    "total_potential_profit_usd",
    "avg_potential_profit_usd",
    "hit_10_pct_share",
    "hit_20_pct_share",
    "hit_50_pct_share",
    "first_touch_total_profit_usd",
    "first_touch_avg_profit_usd",
    "first_touch_win_rate",
)


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


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _first_float(row: dict[str, object], names: tuple[str, ...]) -> float | None:
    lower = {str(key).strip().lower(): value for key, value in row.items()}
    for name in names:
        value = _safe_float(lower.get(name.lower()))
        if value is not None:
            return value
    return None


def _empty_profit_potential_payload() -> dict[str, object]:
    return {
        "available": False,
        "source_dir": str(PROFIT_BACKTEST_DIR),
        "by_kind": {},
        "key_kinds": [
            {"kind": kind, "profit_potential": None}
            for kind in PROFIT_POTENTIAL_KINDS
        ],
        "summary": None,
    }


def _empty_signal_kind_profit_potential_payload() -> dict[str, object]:
    payload = _empty_profit_potential_payload()
    payload["key_kinds"] = []
    return payload


def _empty_signal_kind_groups_payload() -> dict[str, object]:
    profit_payload = _empty_signal_kind_profit_potential_payload()
    return {
        "groups": [],
        "focus_groups": {group: [] for group in ("HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER")},
        "high_potential_focus": {**_empty_high_potential_focus(), "profit_potential": profit_payload},
        "profit_potential": profit_payload,
    }


def _normalize_profit_potential_row(row: dict[str, object]) -> tuple[str, dict[str, float | None]] | None:
    kind = str(row.get("kind") or row.get("signal_kind") or row.get("Signal Kind") or "").strip().upper()
    if not kind:
        return None
    aliases: dict[str, tuple[str, ...]] = {
        "avg_max_gain_pct": ("avg_max_gain_pct", "average_max_gain_pct", "mean_max_gain_pct", "avg_gain_pct"),
        "median_max_gain_pct": ("median_max_gain_pct", "med_max_gain_pct", "median_gain_pct"),
        "max_gain_pct": ("max_gain_pct", "best_max_gain_pct", "peak_max_gain_pct"),
        "total_potential_profit_usd": ("total_potential_profit_usd", "potential_profit_total_usd", "total_profit_usd"),
        "avg_potential_profit_usd": ("avg_potential_profit_usd", "potential_profit_avg_usd", "avg_profit_usd"),
        "hit_10_pct_share": ("hit_10_pct_share", "hit_10_share", "hit_10_pct", "hit_10_rate"),
        "hit_20_pct_share": ("hit_20_pct_share", "hit_20_share", "hit_20_pct", "hit_20_rate"),
        "hit_50_pct_share": ("hit_50_pct_share", "hit_50_share", "hit_50_pct", "hit_50_rate"),
        "first_touch_total_profit_usd": ("first_touch_total_profit_usd", "first_touch_profit_total_usd", "ft_total_profit_usd"),
        "first_touch_avg_profit_usd": ("first_touch_avg_profit_usd", "first_touch_profit_avg_usd", "ft_avg_profit_usd"),
        "first_touch_win_rate": ("first_touch_win_rate", "first_touch_win_pct", "ft_win_rate"),
    }
    metrics = {metric: _first_float(row, names) for metric, names in aliases.items()}
    return kind, metrics


def _aggregate_profit_backtest_rows(rows: list[dict[str, object]]) -> dict[str, dict[str, float | None]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        kind = str(row.get("kind") or row.get("signal_kind") or "").strip().upper()
        if kind:
            grouped[kind].append(row)

    result: dict[str, dict[str, float | None]] = {}
    for kind, items in grouped.items():
        gains = [value for value in (_first_float(item, ("max_gain_pct", "max_gain", "gain_pct")) for item in items) if value is not None]
        profits = [value for value in (_first_float(item, ("potential_profit_usd", "profit_usd", "max_profit_usd")) for item in items) if value is not None]
        first_touch_profits = [value for value in (_first_float(item, ("first_touch_profit_usd", "first_touch_pnl_usd", "ft_profit_usd")) for item in items) if value is not None]
        first_touch_wins = [value for value in (_first_float(item, ("first_touch_win", "first_touch_won", "ft_win")) for item in items) if value is not None]
        metrics = {metric: None for metric in PROFIT_POTENTIAL_METRICS}
        if gains:
            metrics.update(
                {
                    "avg_max_gain_pct": round(sum(gains) / len(gains), 4),
                    "median_max_gain_pct": round(float(median(gains)), 4),
                    "max_gain_pct": round(max(gains), 4),
                    "hit_10_pct_share": round(sum(1 for value in gains if value >= 10.0) / len(gains), 4),
                    "hit_20_pct_share": round(sum(1 for value in gains if value >= 20.0) / len(gains), 4),
                    "hit_50_pct_share": round(sum(1 for value in gains if value >= 50.0) / len(gains), 4),
                }
            )
        if profits:
            metrics["total_potential_profit_usd"] = round(sum(profits), 4)
            metrics["avg_potential_profit_usd"] = round(sum(profits) / len(profits), 4)
        if first_touch_profits:
            metrics["first_touch_total_profit_usd"] = round(sum(first_touch_profits), 4)
            metrics["first_touch_avg_profit_usd"] = round(sum(first_touch_profits) / len(first_touch_profits), 4)
        if first_touch_wins:
            metrics["first_touch_win_rate"] = round(sum(1 for value in first_touch_wins if value > 0) / len(first_touch_wins), 4)
        result[kind] = metrics
    return result


def _read_profit_potential_payload() -> dict[str, object]:
    payload = _empty_profit_potential_payload()
    report_dir = PROFIT_BACKTEST_DIR
    by_kind_path = report_dir / PROFIT_BY_KIND_REPORT
    summary_path = report_dir / PROFIT_SUMMARY_REPORT
    by_kind: dict[str, dict[str, float | None]] = {}

    if by_kind_path.exists():
        try:
            with by_kind_path.open("r", encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            for row in rows:
                normalized = _normalize_profit_potential_row(row)
                if normalized is None:
                    continue
                kind, metrics = normalized
                by_kind[kind] = metrics
        except (OSError, csv.Error):
            by_kind = {}

    if not by_kind:
        backtest_path = report_dir / "signal_profit_backtest.csv"
        if backtest_path.exists():
            try:
                with backtest_path.open("r", encoding="utf-8-sig", newline="") as handle:
                    by_kind = _aggregate_profit_backtest_rows(list(csv.DictReader(handle)))
            except (OSError, csv.Error):
                by_kind = {}

    if summary_path.exists():
        try:
            payload["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload["summary"] = None

    payload["available"] = bool(by_kind or payload["summary"])
    payload["by_kind"] = by_kind
    payload["key_kinds"] = [
        {"kind": kind, "profit_potential": by_kind.get(kind)}
        for kind in PROFIT_POTENTIAL_KINDS
    ]
    return payload


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
          AND UPPER(COALESCE(action, '')) != 'EXIT'
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


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        parsed = _safe_float(value)
        return int(parsed) if parsed is not None else None


def _safe_json_object(value: object) -> dict[str, object]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _diagnostic_value(diagnostics: dict[str, object], *names: str) -> object | None:
    for name in names:
        if name in diagnostics:
            return diagnostics[name]
    return None


def _executor_select_expr(columns: set[str], name: str, alias: str | None = None, table_alias: str = "eo") -> str:
    output_name = alias or name
    if name in columns:
        return f"{table_alias}.{name} AS {output_name}"
    return f"NULL AS {output_name}"


def _round4(value: float | None) -> float | None:
    return round(value, 4) if value is not None else None


def _parse_signal_key_parts(signal_key: object) -> dict[str, str | None]:
    parts = str(signal_key or "").split("|")
    return {
        "timeframe": parts[2] if len(parts) > 2 and parts[2] else None,
        "kind": parts[3] if len(parts) > 3 and parts[3] else None,
        "side": parts[4] if len(parts) > 4 and parts[4] else None,
    }


def _taxonomy_payload(kind: object | None) -> dict[str, str | None]:
    normalized = normalize_signal_kind(str(kind or "")) if kind else None
    if not normalized:
        return {"signal_kind": None, "signal_family": None, "signal_focus_group": None}
    return {
        "signal_kind": normalized,
        "signal_family": signal_family(normalized),
        "signal_focus_group": signal_focus_group(normalized),
    }


def _resolve_signal_taxonomy(
    *,
    diagnostics: dict[str, object],
    signal_key: object,
    joined_kind: object | None = None,
) -> dict[str, str | None]:
    diagnostic_kind = _diagnostic_value(diagnostics, "signal_kind", "kind")
    parsed_kind = _parse_signal_key_parts(signal_key).get("kind")
    kind = diagnostic_kind or parsed_kind or joined_kind
    payload = _taxonomy_payload(kind)
    if diagnostic_kind and not payload["signal_kind"]:
        payload["signal_kind"] = str(diagnostic_kind)
    payload["signal_family"] = (
        str(value)
        if (value := _diagnostic_value(diagnostics, "signal_family", "family")) is not None
        else payload["signal_family"]
    )
    payload["signal_focus_group"] = (
        str(value)
        if (value := _diagnostic_value(diagnostics, "signal_focus_group", "focus_group")) is not None
        else payload["signal_focus_group"]
    )
    return payload


def _empty_executor_ledger_payload() -> dict[str, object]:
    return {
        "summary": {
            "total_closed_trades": 0,
            "total_open_trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven_or_flat": 0,
            "win_rate": 0.0,
            "net_r": 0.0,
            "avg_r": 0.0,
            "gross_win_r": 0.0,
            "gross_loss_r": 0.0,
            "profit_factor": None,
            "breakeven_moves": 0,
            "avg_max_gain_r": 0.0,
            "avg_max_drawdown_r": 0.0,
            "closed_trades_today": 0,
        },
        "open_trades": [],
        "closed_trades": [],
        "exit_reasons": [],
    }


def _empty_learning_effectiveness_payload() -> dict[str, object]:
    return {
        "summary": {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven_or_flat": 0,
            "win_rate": 0.0,
            "net_r": 0.0,
            "avg_r": 0.0,
            "gross_win_r": 0.0,
            "gross_loss_r": 0.0,
            "profit_factor": None,
            "avg_max_gain_r": 0.0,
            "avg_max_drawdown_r": 0.0,
            "total_giveback_r": 0.0,
            "avg_giveback_r": 0.0,
            "reached_1r_count": 0,
            "reached_1r_closed_nonpositive_count": 0,
            "reached_1r_closed_nonpositive_share": 0.0,
            "reached_1r_full_sl_count": 0,
            "breakeven_moves": 0,
            "breakeven_save_count": 0,
            "breakeven_save_r": 0.0,
            "stop_loss_after_profit_count": 0,
            "learning_status": "insufficient_data",
        },
        "windows": [],
        "giveback": {
            "total_giveback_r": 0.0,
            "avg_giveback_r": 0.0,
            "reached_1r_count": 0,
            "reached_1r_closed_nonpositive_count": 0,
            "reached_1r_closed_nonpositive_share": 0.0,
            "reached_1r_full_sl_count": 0,
        },
        "problem_patterns": [],
        "by_kind": [],
        "by_timeframe": [],
        "by_exit_reason": [],
        "recent_trades": [],
    }


def _learning_order_key(row: dict[str, object]) -> str:
    return str(row.get("exit_time") or row.get("updated_at") or row.get("entry_time") or "")


def _read_learning_closed_trades(conn: sqlite3.Connection) -> list[dict[str, object]]:
    columns = _table_columns(conn, "executor_trades")
    if not columns or "r_result" not in columns:
        return []

    signal_columns = _table_columns(conn, "signals")
    can_join_signals = "signal_key" in columns and "signal_key" in signal_columns
    signals_table = "(SELECT signal_key, MIN(kind) AS kind, MIN(timeframe) AS timeframe FROM signals GROUP BY signal_key)"
    join_sql = f"LEFT JOIN {signals_table} s ON s.signal_key = et.signal_key" if can_join_signals else ""
    joined_kind_expr = "s.kind AS joined_signal_kind" if can_join_signals and "kind" in signal_columns else "NULL AS joined_signal_kind"
    joined_timeframe_expr = "s.timeframe AS joined_signal_timeframe" if can_join_signals and "timeframe" in signal_columns else "NULL AS joined_signal_timeframe"
    select_columns = [
        _executor_select_expr(columns, name, table_alias="et")
        for name in (
            "trade_key", "signal_key", "symbol", "timeframe", "side", "entry_price", "exit_price",
            "exit_reason", "r_result", "max_gain_r", "max_drawdown_r", "duration_minutes",
            "moved_to_breakeven", "entry_time", "exit_time", "updated_at", "diagnostics_json",
        )
    ]
    select_columns.extend([joined_kind_expr, joined_timeframe_expr])
    order_expr = "et.exit_time" if "exit_time" in columns else ("et.updated_at" if "updated_at" in columns else "et.rowid")
    rows = conn.execute(
        f"""
        SELECT {', '.join(select_columns)}
        FROM executor_trades et
        {join_sql}
        {_closed_trade_where(columns)}
        ORDER BY {order_expr} ASC, et.rowid ASC
        """
    ).fetchall()

    trades: list[dict[str, object]] = []
    for index, row in enumerate(rows):
        r_result = _safe_float(row["r_result"])
        if r_result is None:
            continue
        diagnostics = _safe_json_object(row["diagnostics_json"])
        parsed = _parse_signal_key_parts(row["signal_key"])
        diagnostic_kind = _diagnostic_value(diagnostics, "signal_kind", "kind")
        kind = diagnostic_kind or parsed.get("kind") or row["joined_signal_kind"] or "UNKNOWN"
        timeframe = row["timeframe"] or parsed.get("timeframe") or row["joined_signal_timeframe"] or "UNKNOWN"
        max_gain_r = _safe_float(row["max_gain_r"]) or 0.0
        max_drawdown_r = _safe_float(row["max_drawdown_r"]) or 0.0
        giveback_r = max(max_gain_r - r_result, 0.0)
        trades.append(
            {
                "_index": index,
                "trade_key": str(row["trade_key"] or ""),
                "signal_key": str(row["signal_key"] or ""),
                "symbol": str(row["symbol"] or "UNKNOWN"),
                "timeframe": str(timeframe) if timeframe not in (None, "") else "UNKNOWN",
                "side": str(row["side"]) if row["side"] is not None else parsed.get("side"),
                "signal_kind": str(kind or "UNKNOWN"),
                "entry_price": _safe_float(row["entry_price"]),
                "exit_price": _safe_float(row["exit_price"]),
                "r_result": r_result,
                "max_gain_r": max_gain_r,
                "max_drawdown_r": max_drawdown_r,
                "giveback_r": giveback_r,
                "moved_to_breakeven": _safe_int(row["moved_to_breakeven"]) == 1,
                "exit_reason": str(row["exit_reason"] or "UNKNOWN"),
                "duration_minutes": _safe_float(row["duration_minutes"]),
                "entry_time": str(row["entry_time"]) if row["entry_time"] is not None else None,
                "exit_time": str(row["exit_time"]) if row["exit_time"] is not None else None,
                "updated_at": str(row["updated_at"]) if row["updated_at"] is not None else None,
            }
        )
    return trades


def _learning_metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    total = len(rows)
    r_values = [float(row["r_result"] or 0.0) for row in rows]
    wins = sum(1 for value in r_values if value > 0.000001)
    losses = sum(1 for value in r_values if value < -0.000001)
    flats = total - wins - losses
    gross_win = sum(value for value in r_values if value > 0)
    gross_loss = abs(sum(value for value in r_values if value < 0))
    max_gain_values = [float(row.get("max_gain_r") or 0.0) for row in rows]
    max_drawdown_values = [float(row.get("max_drawdown_r") or 0.0) for row in rows]
    givebacks = [float(row.get("giveback_r") or 0.0) for row in rows]
    reached_1r_rows = [row for row in rows if float(row.get("max_gain_r") or 0.0) >= 1.0]
    reached_nonpositive = [row for row in reached_1r_rows if float(row.get("r_result") or 0.0) <= 0]
    breakeven_save_rows = [row for row in rows if row.get("moved_to_breakeven") and float(row.get("r_result") or 0.0) >= 0]
    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "breakeven_or_flat": flats,
        "win_rate": _round4(wins / total) if total else 0.0,
        "net_r": _round4(sum(r_values)) or 0.0,
        "avg_r": _round4(sum(r_values) / total) if total else 0.0,
        "gross_win_r": _round4(gross_win) or 0.0,
        "gross_loss_r": _round4(gross_loss) or 0.0,
        "profit_factor": _round4(gross_win / gross_loss) if gross_loss > 0 else None,
        "avg_max_gain_r": _round4(sum(max_gain_values) / total) if total else 0.0,
        "avg_max_drawdown_r": _round4(sum(max_drawdown_values) / total) if total else 0.0,
        "total_giveback_r": _round4(sum(givebacks)) or 0.0,
        "avg_giveback_r": _round4(sum(givebacks) / total) if total else 0.0,
        "reached_1r_count": len(reached_1r_rows),
        "reached_1r_closed_nonpositive_count": len(reached_nonpositive),
        "reached_1r_closed_nonpositive_share": _round4(len(reached_nonpositive) / len(reached_1r_rows)) if reached_1r_rows else 0.0,
        "reached_1r_full_sl_count": sum(1 for row in reached_1r_rows if float(row.get("r_result") or 0.0) <= -0.9),
        "breakeven_moves": sum(1 for row in rows if row.get("moved_to_breakeven")),
        "breakeven_save_count": len(breakeven_save_rows),
        "breakeven_save_r": _round4(sum(max(float(row.get("r_result") or 0.0), 0.0) for row in breakeven_save_rows)) or 0.0,
        "stop_loss_after_profit_count": sum(
            1 for row in rows
            if "stop_loss" in str(row.get("exit_reason") or "").lower() and float(row.get("max_gain_r") or 0.0) > 0.5
        ),
    }


def _learning_status(rows: list[dict[str, object]], summary: dict[str, object]) -> str:
    total = int(summary.get("total_trades") or 0)
    avg_r = float(summary.get("avg_r") or 0.0)
    profit_factor = summary.get("profit_factor")
    pf = float(profit_factor) if profit_factor is not None else None
    if total < 20:
        return "insufficient_data"
    if total >= 50 and avg_r >= 0.1 and pf is not None and pf >= 1.2:
        return "strong_positive_edge"
    if total >= 30 and avg_r > 0 and pf is not None and pf > 1:
        return "positive_edge"
    if avg_r < 0 and pf is not None and pf < 1:
        return "negative_edge"
    if total >= 20:
        recent = rows[-10:]
        previous = rows[-20:-10]
        if previous:
            recent_avg = sum(float(row.get("r_result") or 0.0) for row in recent) / len(recent)
            previous_avg = sum(float(row.get("r_result") or 0.0) for row in previous) / len(previous)
            if recent_avg > previous_avg and float(summary.get("net_r") or 0.0) <= 0:
                return "improving_watch"
    return "insufficient_data"


def _learning_window(name: str, rows: list[dict[str, object]]) -> dict[str, object]:
    metrics = _learning_metrics(rows)
    return {
        "name": name,
        "total_trades": metrics["total_trades"],
        "net_r": metrics["net_r"],
        "avg_r": metrics["avg_r"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "avg_giveback_r": metrics["avg_giveback_r"],
        "reached_1r_closed_nonpositive_count": metrics["reached_1r_closed_nonpositive_count"],
        "breakeven_moves": metrics["breakeven_moves"],
        "full_sl_count": sum(1 for row in rows if float(row.get("r_result") or 0.0) <= -0.9),
    }


def _learning_windows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    half = len(rows) // 2
    windows = [_learning_window("all_time", rows)]
    if half > 0:
        windows.append(_learning_window("first_half", rows[:half]))
        windows.append(_learning_window("second_half", rows[half:]))
    for size in (10, 20, 50):
        if len(rows) >= size:
            windows.append(_learning_window(f"last_{size}", rows[-size:]))
    return windows


def _learning_group_rows(rows: list[dict[str, object]], key_name: str, output_name: str) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key_name) or "UNKNOWN")].append(row)
    output: list[dict[str, object]] = []
    for key, group in grouped.items():
        metrics = _learning_metrics(group)
        output.append(
            {
                output_name: key,
                "total_trades": metrics["total_trades"],
                "wins": metrics["wins"],
                "losses": metrics["losses"],
                "win_rate": metrics["win_rate"],
                "net_r": metrics["net_r"],
                "avg_r": metrics["avg_r"],
                "profit_factor": metrics["profit_factor"],
                "avg_max_gain_r": metrics["avg_max_gain_r"],
                "avg_giveback_r": metrics["avg_giveback_r"],
                "reached_1r_closed_nonpositive_count": metrics["reached_1r_closed_nonpositive_count"],
            }
        )
    return sorted(output, key=lambda row: (-int(row["total_trades"]), str(row[output_name])))


def _learning_exit_reason_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("exit_reason") or "UNKNOWN")].append(row)
    output: list[dict[str, object]] = []
    for reason, group in grouped.items():
        metrics = _learning_metrics(group)
        output.append(
            {
                "exit_reason": reason,
                "total": metrics["total_trades"],
                "wins": metrics["wins"],
                "losses": metrics["losses"],
                "net_r": metrics["net_r"],
                "avg_r": metrics["avg_r"],
                "avg_max_gain_r": metrics["avg_max_gain_r"],
                "avg_giveback_r": metrics["avg_giveback_r"],
            }
        )
    return sorted(output, key=lambda row: (-int(row["total"]), str(row["exit_reason"])))


def _learning_problem_patterns(rows: list[dict[str, object]], summary: dict[str, object], by_exit_reason: list[dict[str, object]]) -> list[dict[str, object]]:
    patterns: list[dict[str, object]] = []
    reached_failures = [row for row in rows if float(row.get("max_gain_r") or 0.0) >= 1.0 and float(row.get("r_result") or 0.0) <= 0]
    if reached_failures:
        examples = ", ".join(str(row.get("symbol") or "UNKNOWN") for row in reached_failures[:3])
        patterns.append({
            "pattern": "large_giveback_after_1r",
            "severity": "critical" if len(reached_failures) >= 3 else "warning",
            "count": len(reached_failures),
            "examples": [
                {
                    "symbol": row.get("symbol"),
                    "timeframe": row.get("timeframe"),
                    "signal_kind": row.get("signal_kind"),
                    "r_result": _round4(float(row.get("r_result") or 0.0)),
                    "max_gain_r": _round4(float(row.get("max_gain_r") or 0.0)),
                    "giveback_r": _round4(float(row.get("giveback_r") or 0.0)),
                }
                for row in reached_failures[:3]
            ],
            "description": f"{len(reached_failures)} trades reached at least +1R and closed non-positive. Examples: {examples}.",
            "suggested_next_step": "Review management diagnostics for these trades before changing any stop or target rules.",
        })
    positive = [float(row.get("r_result") or 0.0) for row in rows if float(row.get("r_result") or 0.0) > 0]
    if positive and (sum(positive) / len(positive)) < 0.25:
        patterns.append({
            "pattern": "wins_too_small",
            "severity": "warning",
            "count": len(positive),
            "description": "Average winning trade is below +0.25R.",
            "suggested_next_step": "Inspect exit reasons and max-gain distribution to see whether winners are being capped too early.",
        })
    negative_abs = [abs(float(row.get("r_result") or 0.0)) for row in rows if float(row.get("r_result") or 0.0) < 0]
    if negative_abs and (sum(negative_abs) / len(negative_abs)) > 0.75:
        patterns.append({
            "pattern": "losses_too_large",
            "severity": "warning",
            "count": len(negative_abs),
            "description": "Average losing trade is larger than -0.75R.",
            "suggested_next_step": "Review whether losses are concentrated in specific kinds, timeframes, or exit reasons.",
        })
    if by_exit_reason:
        top_reason = by_exit_reason[0]
        if str(top_reason.get("exit_reason") or "").lower() == "exit_stop_loss_hit" and float(summary.get("net_r") or 0.0) < 0:
            patterns.append({
                "pattern": "stop_loss_dominates_exits",
                "severity": "critical",
                "count": int(top_reason.get("total") or 0),
                "description": "EXIT_STOP_LOSS_HIT is the most frequent exit reason while net R is negative.",
                "suggested_next_step": "Investigate signal kinds and timeframes that most often end in stop-loss exits.",
            })
    if int(summary.get("total_trades") or 0) < 30:
        patterns.append({
            "pattern": "insufficient_executor_sample",
            "severity": "info",
            "count": int(summary.get("total_trades") or 0),
            "description": "Fewer than 30 closed executor trades are available, so edge claims are not statistically stable.",
            "suggested_next_step": "Keep collecting paper executor outcomes before interpreting profitability as learned edge.",
        })
    return patterns


EXIT_SIMULATOR_RULES: tuple[dict[str, str], ...] = (
    {
        "rule_id": "current_actual",
        "label": "Current Actual",
        "description": "Baseline using the executor trade's recorded actual R result.",
    },
    {
        "rule_id": "no_full_loss_after_1r",
        "label": "No Full Loss After 1R",
        "description": "If a trade reached +1R and actually closed negative, simulate a flat 0R exit instead.",
    },
    {
        "rule_id": "lock_0_25r_after_1r",
        "label": "Lock +0.25R After 1R",
        "description": "If a trade reached +1R, simulate never closing below +0.25R.",
    },
    {
        "rule_id": "lock_0_5r_after_1_5r",
        "label": "Lock +0.5R After 1.5R",
        "description": "If a trade reached +1.5R, simulate never closing below +0.5R; if it reached +1R, simulate never closing below flat.",
    },
    {
        "rule_id": "trailing_40pct_giveback_after_1r",
        "label": "Trail 40% Giveback After 1R",
        "description": "If a trade reached +1R, simulate retaining 60% of the maximum favorable R excursion.",
    },
    {
        "rule_id": "trailing_50pct_giveback_after_1r",
        "label": "Trail 50% Giveback After 1R",
        "description": "If a trade reached +1R, simulate retaining 50% of the maximum favorable R excursion.",
    },
    {
        "rule_id": "take_half_at_1r_rest_actual",
        "label": "Take Half At 1R",
        "description": "If a trade reached +1R, simulate half closing at +1R while the other half follows the actual outcome.",
    },
    {
        "rule_id": "conservative_protect_after_0_75r",
        "label": "Protect After 0.75R",
        "description": "If a trade reached +0.75R and actually closed negative, simulate a flat 0R exit instead.",
    },
)


def _empty_exit_simulator_payload() -> dict[str, object]:
    rules = [
        {
            "rule_id": rule["rule_id"],
            "label": rule["label"],
            "description": rule["description"],
            "total_trades": 0,
            "simulated_net_r": 0.0,
            "simulated_avg_r": 0.0,
            "simulated_wins": 0,
            "simulated_losses": 0,
            "simulated_win_rate": 0.0,
            "simulated_profit_factor": None,
            "delta_net_r_vs_actual": 0.0,
            "delta_avg_r_vs_actual": 0.0,
            "prevented_full_losses": 0,
            "improved_trades": 0,
            "worsened_trades": 0,
            "avg_simulated_giveback_r": 0.0,
            "recommendation": "baseline" if rule["rule_id"] == "current_actual" else "needs_more_data",
        }
        for rule in EXIT_SIMULATOR_RULES
    ]
    return {
        "summary": {
            "total_trades": 0,
            "current_net_r": 0.0,
            "current_avg_r": 0.0,
            "current_profit_factor": None,
            "current_wins": 0,
            "current_losses": 0,
            "current_win_rate": 0.0,
            "avg_max_gain_r": 0.0,
            "avg_giveback_r": 0.0,
            "best_rule_by_net_r": None,
            "best_rule_by_profit_factor": None,
            "best_simulated_net_r": 0.0,
            "best_delta_net_r": 0.0,
            "sample_warning": {
                "warning": True,
                "message": "Sample is small; use this as diagnostics only.",
            },
        },
        "rules": rules,
        "by_kind": [],
        "by_timeframe": [],
        "trade_simulations": [],
    }


def _simulate_exit_rule(rule_id: str, r_result: float, max_gain_r: float) -> float:
    if rule_id == "current_actual":
        return r_result
    if rule_id == "no_full_loss_after_1r":
        return 0.0 if max_gain_r >= 1.0 and r_result < 0 else r_result
    if rule_id == "lock_0_25r_after_1r":
        return max(r_result, 0.25) if max_gain_r >= 1.0 else r_result
    if rule_id == "lock_0_5r_after_1_5r":
        if max_gain_r >= 1.5:
            return max(r_result, 0.5)
        if max_gain_r >= 1.0:
            return max(r_result, 0.0)
        return r_result
    if rule_id == "trailing_40pct_giveback_after_1r":
        return max(r_result, max_gain_r * 0.60) if max_gain_r >= 1.0 else r_result
    if rule_id == "trailing_50pct_giveback_after_1r":
        return max(r_result, max_gain_r * 0.50) if max_gain_r >= 1.0 else r_result
    if rule_id == "take_half_at_1r_rest_actual":
        return 0.5 + (0.5 * r_result) if max_gain_r >= 1.0 else r_result
    if rule_id == "conservative_protect_after_0_75r":
        return 0.0 if max_gain_r >= 0.75 and r_result < 0 else r_result
    return r_result


def _exit_simulator_current_metrics(rows: list[dict[str, object]]) -> dict[str, object]:
    total = len(rows)
    actual = [float(row.get("r_result") or 0.0) for row in rows]
    gross_win = sum(value for value in actual if value > 0)
    gross_loss = abs(sum(value for value in actual if value < 0))
    wins = sum(1 for value in actual if value > 0.000001)
    losses = sum(1 for value in actual if value < -0.000001)
    max_gains = [float(row.get("max_gain_r") or 0.0) for row in rows]
    givebacks = [max(float(row.get("max_gain_r") or 0.0) - float(row.get("r_result") or 0.0), 0.0) for row in rows]
    return {
        "total_trades": total,
        "current_net_r": _round4(sum(actual)) or 0.0,
        "current_avg_r": _round4(sum(actual) / total) if total else 0.0,
        "current_profit_factor": _round4(gross_win / gross_loss) if gross_loss > 0 else None,
        "current_wins": wins,
        "current_losses": losses,
        "current_win_rate": _round4(wins / total) if total else 0.0,
        "avg_max_gain_r": _round4(sum(max_gains) / total) if total else 0.0,
        "avg_giveback_r": _round4(sum(givebacks) / total) if total else 0.0,
    }


def _exit_rule_metrics(rule: dict[str, str], rows: list[dict[str, object]], current_net_r: float, current_avg_r: float) -> dict[str, object]:
    rule_id = rule["rule_id"]
    total = len(rows)
    simulated = [_simulate_exit_rule(rule_id, float(row.get("r_result") or 0.0), float(row.get("max_gain_r") or 0.0)) for row in rows]
    gross_win = sum(value for value in simulated if value > 0)
    gross_loss = abs(sum(value for value in simulated if value < 0))
    wins = sum(1 for value in simulated if value > 0.000001)
    losses = sum(1 for value in simulated if value < -0.000001)
    net_r = sum(simulated)
    avg_r = net_r / total if total else 0.0
    pf = gross_win / gross_loss if gross_loss > 0 else None
    improved = 0
    worsened = 0
    prevented = 0
    giveback_total = 0.0
    for row, sim_r in zip(rows, simulated):
        actual = float(row.get("r_result") or 0.0)
        max_gain = float(row.get("max_gain_r") or 0.0)
        if sim_r > actual + 0.000001:
            improved += 1
        if sim_r < actual - 0.000001:
            worsened += 1
        if actual <= -0.9 and sim_r > actual + 0.000001:
            prevented += 1
        giveback_total += max(max_gain - sim_r, 0.0)
    recommendation = "baseline"
    if rule_id != "current_actual":
        if total < 30:
            recommendation = "needs_more_data"
        elif net_r > 0 and pf is not None and pf > 1:
            recommendation = "strong_candidate"
        elif net_r > current_net_r:
            recommendation = "candidate"
        else:
            recommendation = "diagnostic_only"
    return {
        "rule_id": rule_id,
        "label": rule["label"],
        "description": rule["description"],
        "total_trades": total,
        "simulated_net_r": _round4(net_r) or 0.0,
        "simulated_avg_r": _round4(avg_r) if total else 0.0,
        "simulated_wins": wins,
        "simulated_losses": losses,
        "simulated_win_rate": _round4(wins / total) if total else 0.0,
        "simulated_profit_factor": _round4(pf) if pf is not None else None,
        "delta_net_r_vs_actual": _round4(net_r - current_net_r) or 0.0,
        "delta_avg_r_vs_actual": _round4(avg_r - current_avg_r) if total else 0.0,
        "prevented_full_losses": prevented,
        "improved_trades": improved,
        "worsened_trades": worsened,
        "avg_simulated_giveback_r": _round4(giveback_total / total) if total else 0.0,
        "recommendation": recommendation,
    }


def _exit_simulator_group_rows(rows: list[dict[str, object]], key_name: str, output_name: str) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key_name) or "UNKNOWN")].append(row)
    output: list[dict[str, object]] = []
    for key, group in grouped.items():
        current = _exit_simulator_current_metrics(group)
        rule_rows = [_exit_rule_metrics(rule, group, float(current["current_net_r"] or 0.0), float(current["current_avg_r"] or 0.0)) for rule in EXIT_SIMULATOR_RULES]
        best = max(rule_rows, key=lambda item: (float(item.get("simulated_net_r") or 0.0), str(item.get("rule_id") or ""))) if rule_rows else None
        output.append({
            output_name: key,
            "total_trades": current["total_trades"],
            "current_net_r": current["current_net_r"],
            "best_rule_id": best.get("rule_id") if best else None,
            "best_simulated_net_r": best.get("simulated_net_r") if best else 0.0,
            "best_delta_net_r": best.get("delta_net_r_vs_actual") if best else 0.0,
            "avg_giveback_r": current["avg_giveback_r"],
            "reached_1r_count": sum(1 for row in group if float(row.get("max_gain_r") or 0.0) >= 1.0),
        })
    return sorted(output, key=lambda row: (-int(row["total_trades"]), str(row[output_name])))


def _exit_simulator_trade_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in sorted(rows, key=_learning_order_key, reverse=True)[:50]:
        actual = float(row.get("r_result") or 0.0)
        max_gain = float(row.get("max_gain_r") or 0.0)
        simulations = [(rule["rule_id"], _simulate_exit_rule(rule["rule_id"], actual, max_gain)) for rule in EXIT_SIMULATOR_RULES]
        best_rule_id, best_r = max(simulations, key=lambda item: (item[1], item[0]))
        output.append({
            "symbol": row.get("symbol") or "UNKNOWN",
            "timeframe": row.get("timeframe") or "UNKNOWN",
            "signal_kind": row.get("signal_kind") or "UNKNOWN",
            "side": row.get("side"),
            "actual_r": _round4(actual) or 0.0,
            "max_gain_r": _round4(max_gain) or 0.0,
            "actual_giveback_r": _round4(max(max_gain - actual, 0.0)) or 0.0,
            "best_rule_id_for_trade": best_rule_id,
            "best_simulated_r_for_trade": _round4(best_r) or 0.0,
            "best_delta_r_for_trade": _round4(best_r - actual) or 0.0,
            "exit_reason": row.get("exit_reason") or "UNKNOWN",
            "moved_to_breakeven": bool(row.get("moved_to_breakeven")),
            "exit_time": row.get("exit_time"),
        })
    return output


def _read_executor_exit_simulator() -> dict[str, object]:
    payload = _empty_exit_simulator_payload()
    if not SIGNALS_DB_PATH.exists():
        return payload

    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = _read_learning_closed_trades(conn)
    except sqlite3.Error:
        return payload
    finally:
        conn.close()

    if not rows:
        return payload

    current = _exit_simulator_current_metrics(rows)
    current_net = float(current["current_net_r"] or 0.0)
    current_avg = float(current["current_avg_r"] or 0.0)
    rules = [_exit_rule_metrics(rule, rows, current_net, current_avg) for rule in EXIT_SIMULATOR_RULES]
    best_by_net = max(rules, key=lambda item: (float(item.get("simulated_net_r") or 0.0), str(item.get("rule_id") or "")))
    best_by_pf = max(
        rules,
        key=lambda item: (
            -1.0 if item.get("simulated_profit_factor") is None else float(item.get("simulated_profit_factor") or 0.0),
            float(item.get("simulated_net_r") or 0.0),
            str(item.get("rule_id") or ""),
        ),
    )
    payload.update({
        "summary": {
            **current,
            "best_rule_by_net_r": best_by_net.get("rule_id"),
            "best_rule_by_profit_factor": best_by_pf.get("rule_id"),
            "best_simulated_net_r": best_by_net.get("simulated_net_r"),
            "best_delta_net_r": best_by_net.get("delta_net_r_vs_actual"),
            "sample_warning": {
                "warning": int(current.get("total_trades") or 0) < 30,
                "message": "Sample is small; use this as diagnostics only.",
            },
        },
        "rules": rules,
        "by_kind": _exit_simulator_group_rows(rows, "signal_kind", "kind"),
        "by_timeframe": _exit_simulator_group_rows(rows, "timeframe", "timeframe"),
        "trade_simulations": _exit_simulator_trade_rows(rows),
    })
    return payload

def _read_learning_effectiveness() -> dict[str, object]:
    payload = _empty_learning_effectiveness_payload()
    if not SIGNALS_DB_PATH.exists():
        return payload

    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        rows = _read_learning_closed_trades(conn)
    except sqlite3.Error:
        return payload
    finally:
        conn.close()

    if not rows:
        return payload

    summary = _learning_metrics(rows)
    summary["learning_status"] = _learning_status(rows, summary)
    by_exit_reason = _learning_exit_reason_rows(rows)
    payload.update(
        {
            "summary": summary,
            "windows": _learning_windows(rows),
            "giveback": {
                "total_giveback_r": summary["total_giveback_r"],
                "avg_giveback_r": summary["avg_giveback_r"],
                "reached_1r_count": summary["reached_1r_count"],
                "reached_1r_closed_nonpositive_count": summary["reached_1r_closed_nonpositive_count"],
                "reached_1r_closed_nonpositive_share": summary["reached_1r_closed_nonpositive_share"],
                "reached_1r_full_sl_count": summary["reached_1r_full_sl_count"],
            },
            "problem_patterns": _learning_problem_patterns(rows, summary, by_exit_reason),
            "by_kind": _learning_group_rows(rows, "signal_kind", "kind"),
            "by_timeframe": _learning_group_rows(rows, "timeframe", "timeframe"),
            "by_exit_reason": by_exit_reason,
            "recent_trades": [
                {
                    key: (_round4(value) if isinstance(value, float) else value)
                    for key, value in row.items()
                    if not key.startswith("_") and key != "updated_at"
                }
                for row in sorted(rows, key=_learning_order_key, reverse=True)[:20]
            ],
        }
    )
    return payload


def _closed_trade_where(columns: set[str]) -> str:
    clauses: list[str] = []
    if "exit_time" in columns:
        clauses.append("et.exit_time IS NOT NULL")
    if "state" in columns:
        clauses.append("UPPER(COALESCE(et.state, '')) = 'EXITED'")
    return f"WHERE ({' OR '.join(clauses)})" if clauses else ""


def _read_executor_ledger(limit: int = 50) -> dict[str, object]:
    payload = _empty_executor_ledger_payload()
    if not SIGNALS_DB_PATH.exists():
        return payload

    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        payload["open_trades"] = _read_executor_ledger_open_trades(conn, limit=limit)
        payload["closed_trades"] = _read_executor_ledger_closed_trades(conn, limit=limit)
        payload["exit_reasons"] = _read_executor_ledger_exit_reasons(conn)
        payload["summary"] = _read_executor_ledger_summary(conn, payload["open_trades"])
        return payload
    finally:
        conn.close()


def _read_executor_ledger_summary(conn: sqlite3.Connection, open_trades: object) -> dict[str, object]:
    summary = dict(_empty_executor_ledger_payload()["summary"])
    summary["total_open_trades"] = len(open_trades) if isinstance(open_trades, list) else 0

    columns = _table_columns(conn, "executor_trades")
    if "r_result" not in columns:
        return summary

    where_sql = _closed_trade_where(columns)
    rows = conn.execute(
        f"""
        SELECT
            r_result,
            {_executor_select_expr(columns, 'moved_to_breakeven', table_alias='et')},
            {_executor_select_expr(columns, 'max_gain_r', table_alias='et')},
            {_executor_select_expr(columns, 'max_drawdown_r', table_alias='et')}
        FROM executor_trades et
        {where_sql}
        """
    ).fetchall()
    r_values = [_safe_float(row["r_result"]) for row in rows]
    r_values = [value for value in r_values if value is not None]
    total_closed = len(r_values)
    wins = sum(1 for value in r_values if value > 0.0000001)
    losses = sum(1 for value in r_values if value < -0.0000001)
    flats = total_closed - wins - losses
    non_flat = wins + losses
    gross_win = sum(value for value in r_values if value > 0)
    gross_loss = abs(sum(value for value in r_values if value < 0))
    max_gain_values = [value for row in rows if (value := _safe_float(row["max_gain_r"])) is not None]
    max_drawdown_values = [value for row in rows if (value := _safe_float(row["max_drawdown_r"])) is not None]

    summary.update(
        {
            "total_closed_trades": total_closed,
            "wins": wins,
            "losses": losses,
            "breakeven_or_flat": flats,
            "win_rate": _round4(wins / (non_flat or total_closed)) if total_closed else 0.0,
            "net_r": _round4(sum(r_values)) or 0.0,
            "avg_r": _round4(sum(r_values) / total_closed) if total_closed else 0.0,
            "gross_win_r": _round4(gross_win) or 0.0,
            "gross_loss_r": _round4(gross_loss) or 0.0,
            "profit_factor": _round4(gross_win / gross_loss) if gross_loss > 0 else None,
            "breakeven_moves": sum(1 for row in rows if _safe_int(row["moved_to_breakeven"]) == 1),
            "avg_max_gain_r": _round4(sum(max_gain_values) / len(max_gain_values)) if max_gain_values else 0.0,
            "avg_max_drawdown_r": _round4(sum(max_drawdown_values) / len(max_drawdown_values)) if max_drawdown_values else 0.0,
            "closed_trades_today": _executor_closed_trades_today(conn),
        }
    )
    return summary


def _read_executor_ledger_closed_trades(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, object]]:
    columns = _table_columns(conn, "executor_trades")
    if not columns:
        return []
    signal_columns = _table_columns(conn, "signals")
    can_join_signals = {"signal_key", "kind"}.issubset(signal_columns)
    join_sql = "LEFT JOIN signals s ON s.signal_key = et.signal_key" if can_join_signals else ""
    kind_expr = "s.kind AS joined_signal_kind" if can_join_signals else "NULL AS joined_signal_kind"
    select_columns = [
        _executor_select_expr(columns, name, table_alias="et")
        for name in (
            "trade_key", "signal_key", "symbol", "timeframe", "side", "state", "entry_price", "exit_price",
            "initial_sl", "final_sl", "current_sl", "exit_reason", "r_result", "max_gain_r",
            "max_drawdown_r", "bars_in_trade", "duration_minutes", "moved_to_breakeven", "breakeven_time",
            "entry_time", "exit_time", "updated_at", "diagnostics_json",
        )
    ]
    select_columns.append(kind_expr)
    order_expr = "et.exit_time" if "exit_time" in columns else ("et.updated_at" if "updated_at" in columns else "et.rowid")
    rows = conn.execute(
        f"""
        SELECT {', '.join(select_columns)}
        FROM executor_trades et
        {join_sql}
        {_closed_trade_where(columns)}
        ORDER BY {order_expr} DESC, et.rowid DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    trades: list[dict[str, object]] = []
    for row in rows:
        diagnostics = _safe_json_object(row["diagnostics_json"])
        taxonomy = _resolve_signal_taxonomy(diagnostics=diagnostics, signal_key=row["signal_key"], joined_kind=row["joined_signal_kind"])
        parsed = _parse_signal_key_parts(row["signal_key"])
        trades.append({
            "trade_key": str(row["trade_key"] or ""),
            "signal_key": str(row["signal_key"] or ""),
            "symbol": str(row["symbol"] or "UNKNOWN"),
            "timeframe": str(row["timeframe"] or parsed.get("timeframe") or "") or None,
            "side": str(row["side"]) if row["side"] is not None else None,
            "state": str(row["state"]) if row["state"] is not None else None,
            "entry_price": _safe_float(row["entry_price"]),
            "exit_price": _safe_float(row["exit_price"]),
            "initial_sl": _safe_float(row["initial_sl"]),
            "final_sl": _safe_float(row["final_sl"]),
            "current_sl": _safe_float(row["current_sl"]),
            "exit_reason": str(row["exit_reason"]) if row["exit_reason"] is not None else None,
            "r_result": _round4(_safe_float(row["r_result"])),
            "max_gain_r": _round4(_safe_float(row["max_gain_r"])),
            "max_drawdown_r": _round4(_safe_float(row["max_drawdown_r"])),
            "bars_in_trade": _safe_int(row["bars_in_trade"]),
            "duration_minutes": _round4(_safe_float(row["duration_minutes"])),
            "moved_to_breakeven": bool(_safe_int(row["moved_to_breakeven"])),
            "breakeven_time": str(row["breakeven_time"]) if row["breakeven_time"] is not None else None,
            "entry_time": str(row["entry_time"]) if row["entry_time"] is not None else None,
            "exit_time": str(row["exit_time"]) if row["exit_time"] is not None else None,
            "updated_at": str(row["updated_at"]) if row["updated_at"] is not None else None,
            **taxonomy,
        })
    return trades


def _executor_breakeven_active(side: object, state: object, entry_price: object, current_sl: object) -> bool:
    state_text = str(state or "").strip().upper()
    if state_text in {"PROTECT_BREAKEVEN", "TRAILING_PROFIT"}:
        return True

    entry = _safe_float(entry_price)
    stop = _safe_float(current_sl)
    if entry is None or stop is None:
        return False

    side_text = str(side or "").strip().upper()
    if side_text in {"BUY", "LONG"}:
        return stop >= entry
    if side_text in {"SELL", "SHORT"}:
        return stop <= entry
    return False


def _executor_breakeven_display_fields(
    *, side: object, state: object, entry_price: object, current_sl: object, diagnostics: dict[str, object]
) -> dict[str, object]:
    breakeven_time = _diagnostic_value(diagnostics, "breakeven_time")
    breakeven_active = _executor_breakeven_active(side, state, entry_price, current_sl)
    has_breakeven_time = breakeven_time not in (None, "")
    return {
        "breakeven_active": breakeven_active,
        "breakeven_display_time": str(breakeven_time) if breakeven_active and has_breakeven_time else None,
        "stale_breakeven_time": bool(has_breakeven_time and not breakeven_active),
    }


def _read_executor_ledger_open_trades(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, object]]:
    outcome_columns = _table_columns(conn, "executor_outcomes")
    if not {"state", "action"}.issubset(outcome_columns):
        return []
    signal_columns = _table_columns(conn, "signals")
    can_join_timeframe = {"signal_key", "timeframe"}.issubset(signal_columns)
    can_join_kind = {"signal_key", "kind"}.issubset(signal_columns)
    join_sql = "LEFT JOIN signals s ON s.signal_key = eo.signal_key" if (can_join_timeframe or can_join_kind) else ""
    timeframe_expr = "s.timeframe AS timeframe" if can_join_timeframe else "NULL AS timeframe"
    kind_expr = "s.kind AS joined_signal_kind" if can_join_kind else "NULL AS joined_signal_kind"
    select_columns = [
        _executor_select_expr(outcome_columns, name)
        for name in (
            "signal_key", "symbol", "side", "state", "action", "reason", "entry_price", "current_sl",
            "exit_price", "max_gain_r", "max_drawdown_r", "bars_in_trade", "updated_at", "created_at", "diagnostics_json",
        )
    ]
    select_columns.extend([timeframe_expr, kind_expr])
    order_expr = "eo.updated_at" if "updated_at" in outcome_columns else "eo.rowid"
    rows = conn.execute(
        f"""
        SELECT {', '.join(select_columns)}
        FROM executor_outcomes eo
        {join_sql}
        WHERE UPPER(COALESCE(eo.state, '')) NOT IN ('EXITED', 'TRADE_WATCH')
          AND UPPER(COALESCE(eo.action, '')) NOT IN ('EXIT', 'WATCH')
          AND (
              UPPER(COALESCE(eo.state, '')) IN ('ENTERED', 'PROTECT_BREAKEVEN', 'TRAILING_PROFIT')
              OR UPPER(COALESCE(eo.action, '')) = 'HOLD'
          )
        ORDER BY {order_expr} DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    trades: list[dict[str, object]] = []
    for row in rows:
        diagnostics = _safe_json_object(row["diagnostics_json"])
        taxonomy = _resolve_signal_taxonomy(diagnostics=diagnostics, signal_key=row["signal_key"], joined_kind=row["joined_signal_kind"])
        parsed = _parse_signal_key_parts(row["signal_key"])
        side = str(row["side"]) if row["side"] is not None else None
        state = str(row["state"]) if row["state"] is not None else None
        entry_price = _safe_float(row["entry_price"])
        current_sl = _safe_float(row["current_sl"])
        breakeven_fields = _executor_breakeven_display_fields(
            side=side,
            state=state,
            entry_price=entry_price,
            current_sl=current_sl,
            diagnostics=diagnostics,
        )
        trades.append({
            "signal_key": str(row["signal_key"] or ""),
            "symbol": str(row["symbol"] or "UNKNOWN"),
            "timeframe": str(row["timeframe"] or parsed.get("timeframe") or "") or None,
            "side": side,
            "state": state,
            "action": str(row["action"]) if row["action"] is not None else None,
            "reason": str(row["reason"]) if row["reason"] is not None else None,
            "entry_price": entry_price,
            "current_sl": current_sl,
            "exit_price": _safe_float(row["exit_price"]),
            "max_gain_r": _round4(_safe_float(row["max_gain_r"])),
            "max_drawdown_r": _round4(_safe_float(row["max_drawdown_r"])),
            "bars_in_trade": _safe_int(row["bars_in_trade"]),
            "updated_at": str(row["updated_at"]) if row["updated_at"] is not None else None,
            "created_at": str(row["created_at"]) if row["created_at"] is not None else None,
            "executor_entry_time": str(value) if (value := _diagnostic_value(diagnostics, "executor_entry_time", "entry_time")) is not None else None,
            "executor_initial_sl": _safe_float(_diagnostic_value(diagnostics, "executor_initial_sl", "initial_sl")),
            "breakeven_time": str(value) if (value := _diagnostic_value(diagnostics, "breakeven_time")) is not None else None,
            **breakeven_fields,
            **taxonomy,
        })
    return trades


def _read_executor_ledger_exit_reasons(conn: sqlite3.Connection) -> list[dict[str, object]]:
    columns = _table_columns(conn, "executor_trades")
    if not {"exit_reason", "r_result"}.issubset(columns):
        return []
    rows = conn.execute(
        f"""
        SELECT
            COALESCE(exit_reason, 'UNKNOWN') AS exit_reason,
            COUNT(*) AS total,
            SUM(CASE WHEN r_result > 0.0000001 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN r_result < -0.0000001 THEN 1 ELSE 0 END) AS losses,
            SUM(COALESCE(r_result, 0)) AS net_r,
            AVG(r_result) AS avg_r,
            AVG({_executor_select_expr(columns, 'max_gain_r', table_alias='et').split(' AS ')[0]}) AS avg_max_gain_r,
            AVG({_executor_select_expr(columns, 'max_drawdown_r', table_alias='et').split(' AS ')[0]}) AS avg_max_drawdown_r
        FROM executor_trades et
        {_closed_trade_where(columns)}
        GROUP BY COALESCE(exit_reason, 'UNKNOWN')
        ORDER BY total DESC, exit_reason ASC
        """
    ).fetchall()
    return [
        {
            "exit_reason": str(row["exit_reason"] or "UNKNOWN"),
            "total": int(row["total"] or 0),
            "wins": int(row["wins"] or 0),
            "losses": int(row["losses"] or 0),
            "net_r": _round4(_safe_float(row["net_r"])) or 0.0,
            "avg_r": _round4(_safe_float(row["avg_r"])) or 0.0,
            "avg_max_gain_r": _round4(_safe_float(row["avg_max_gain_r"])) or 0.0,
            "avg_max_drawdown_r": _round4(_safe_float(row["avg_max_drawdown_r"])) or 0.0,
        }
        for row in rows
    ]


def _read_executor_active_trades(limit: int = 500) -> ActiveExecutorTradesResponse:
    if not SIGNALS_DB_PATH.exists():
        return ActiveExecutorTradesResponse()

    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        outcome_columns = _table_columns(conn, "executor_outcomes")
        if not {"state", "action"}.issubset(outcome_columns):
            return ActiveExecutorTradesResponse()

        signal_columns = _table_columns(conn, "signals")
        can_join_signals = {"signal_key", "timeframe"}.issubset(signal_columns)
        join_sql = "LEFT JOIN signals s ON s.signal_key = eo.signal_key" if can_join_signals else ""
        timeframe_expr = "s.timeframe AS timeframe" if can_join_signals else "NULL AS timeframe"
        select_columns = [
            _executor_select_expr(outcome_columns, "signal_key"),
            _executor_select_expr(outcome_columns, "symbol"),
            timeframe_expr,
            _executor_select_expr(outcome_columns, "side"),
            _executor_select_expr(outcome_columns, "state"),
            _executor_select_expr(outcome_columns, "action"),
            _executor_select_expr(outcome_columns, "reason"),
            _executor_select_expr(outcome_columns, "entry_price"),
            _executor_select_expr(outcome_columns, "current_sl"),
            _executor_select_expr(outcome_columns, "exit_price"),
            _executor_select_expr(outcome_columns, "max_gain_r"),
            _executor_select_expr(outcome_columns, "max_drawdown_r"),
            _executor_select_expr(outcome_columns, "bars_in_trade"),
            _executor_select_expr(outcome_columns, "updated_at"),
            _executor_select_expr(outcome_columns, "created_at"),
            _executor_select_expr(outcome_columns, "diagnostics_json"),
        ]
        order_expr = "eo.updated_at" if "updated_at" in outcome_columns else "eo.rowid"
        rows = conn.execute(
            f"""
            SELECT {", ".join(select_columns)}
            FROM executor_outcomes eo
            {join_sql}
            WHERE UPPER(COALESCE(eo.state, '')) != 'EXITED'
              AND UPPER(COALESCE(eo.action, '')) != 'EXIT'
              AND (
                  UPPER(COALESCE(eo.state, '')) IN ('ENTERED', 'PROTECT_BREAKEVEN')
                  OR UPPER(COALESCE(eo.action, '')) = 'HOLD'
              )
            ORDER BY {order_expr} DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    trades: list[ActiveExecutorTrade] = []
    for row in rows:
        diagnostics = _safe_json_object(row["diagnostics_json"])
        trade = ActiveExecutorTrade(
            signal_key=str(row["signal_key"] or ""),
            symbol=str(row["symbol"] or "UNKNOWN"),
            timeframe=str(row["timeframe"]) if row["timeframe"] is not None else None,
            side=str(row["side"]) if row["side"] is not None else None,
            state=str(row["state"]) if row["state"] is not None else None,
            action=str(row["action"]) if row["action"] is not None else None,
            reason=str(row["reason"]) if row["reason"] is not None else None,
            entry_price=_safe_float(row["entry_price"]),
            current_sl=_safe_float(row["current_sl"]),
            exit_price=_safe_float(row["exit_price"]),
            max_gain_r=_safe_float(row["max_gain_r"]),
            max_drawdown_r=_safe_float(row["max_drawdown_r"]),
            bars_in_trade=_safe_int(row["bars_in_trade"]),
            updated_at=str(row["updated_at"]) if row["updated_at"] is not None else None,
            created_at=str(row["created_at"]) if row["created_at"] is not None else None,
            executor_entry_time=(
                str(value) if (value := _diagnostic_value(diagnostics, "executor_entry_time", "entry_time")) is not None else None
            ),
            executor_initial_sl=_safe_float(_diagnostic_value(diagnostics, "executor_initial_sl", "initial_sl")),
            breakeven_time=(
                str(value) if (value := _diagnostic_value(diagnostics, "breakeven_time")) is not None else None
            ),
            signal_kind=(str(value) if (value := _diagnostic_value(diagnostics, "signal_kind", "kind")) is not None else None),
            signal_family=(str(value) if (value := _diagnostic_value(diagnostics, "signal_family")) is not None else None),
            signal_focus_group=(
                str(value) if (value := _diagnostic_value(diagnostics, "signal_focus_group", "focus_group")) is not None else None
            ),
        )
        trades.append(trade)

    max_gain_values = [trade.max_gain_r for trade in trades if trade.max_gain_r is not None]
    max_drawdown_values = [trade.max_drawdown_r for trade in trades if trade.max_drawdown_r is not None]
    summary = ActiveExecutorTradesSummary(
        total_open_trades=len(trades),
        protect_breakeven_count=sum(1 for trade in trades if (trade.state or "").upper() == "PROTECT_BREAKEVEN"),
        entered_count=sum(1 for trade in trades if (trade.state or "").upper() == "ENTERED"),
        avg_max_gain_r=round(sum(max_gain_values) / len(max_gain_values), 6) if max_gain_values else None,
        avg_max_drawdown_r=round(sum(max_drawdown_values) / len(max_drawdown_values), 6) if max_drawdown_values else None,
    )
    return ActiveExecutorTradesResponse(rows=trades, summary=summary)


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


def _empty_focus_groups() -> dict[str, list[object]]:
    return {group: [] for group in ("HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER")}


def _empty_high_potential_focus(profit_payload: dict[str, object] | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "high_potential_summary": [],
        "by_kind": [],
        "by_timeframe": [],
        "by_symbol": [],
        "by_kind_timeframe": [],
        "management_recommendations": [],
        "focus_group_comparison": [],
    }
    if profit_payload is not None:
        payload["profit_potential"] = profit_payload
    return payload


def _empty_signal_kind_groups_payload(profit_payload: dict[str, object]) -> dict[str, object]:
    return {
        "groups": [],
        "focus_groups": _empty_focus_groups(),
        "high_potential_focus": _empty_high_potential_focus(profit_payload),
        "profit_potential": profit_payload,
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


def _profit_potential_by_kind(payload: dict[str, object]) -> dict[str, dict[str, float | None]]:
    raw = payload.get("by_kind")
    if not isinstance(raw, dict):
        return {}
    return {str(kind): metrics for kind, metrics in raw.items() if isinstance(metrics, dict)}


def _attach_profit_potential(rows: list[dict[str, object]], profit_payload: dict[str, object]) -> None:
    by_kind = _profit_potential_by_kind(profit_payload)
    for row in rows:
        row["profit_potential"] = by_kind.get(str(row.get("kind", "")).upper())


def _high_potential_focus_payload(rows: list[sqlite3.Row], profit_payload: dict[str, object] | None = None) -> dict[str, object]:
    profit_payload = profit_payload or _read_profit_potential_payload()
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
    _attach_profit_potential(by_kind, profit_payload)
    recommendations = Counter(str(row["recommended_management"]) for row in by_kind if int(row.get("total", 0)) > 0)
    by_timeframe = _aggregate_focus_rows(rows, ("timeframe",), high_potential_only=True)
    by_symbol = _aggregate_focus_rows(rows, ("symbol",), high_potential_only=True)
    by_kind_timeframe = _aggregate_focus_rows(rows, ("kind", "timeframe"), high_potential_only=True)
    _attach_profit_potential(by_kind_timeframe, profit_payload)
    return {
        "high_potential_summary": _aggregate_focus_rows(rows, ("signal_focus_group",), high_potential_only=True),
        "by_kind": by_kind,
        "by_timeframe": by_timeframe,
        "by_symbol": by_symbol,
        "by_kind_timeframe": by_kind_timeframe,
        "management_recommendations": [
            {"recommendation": recommendation, "total_groups": total}
            for recommendation, total in sorted(recommendations.items())
        ],
        "focus_group_comparison": _aggregate_focus_rows(rows, ("signal_focus_group",), high_potential_only=False),
        "profit_potential": profit_payload,
    }


def _signals_metric_table_available() -> bool:
    if not SIGNALS_DB_PATH.exists():
        return False
    conn = sqlite3.connect(str(SIGNALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        columns = _table_columns(conn, "signals")
        required = {"kind", "timeframe", "source", "symbol", "status", "score_last", "score_max", "max_gain_pct", "max_drawdown_pct"}
        return required.issubset(columns)
    finally:
        conn.close()


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


    @app.get("/api/executor-active-trades", response_model=ActiveExecutorTradesResponse)
    async def executor_active_trades(limit: Annotated[int, Query(ge=1, le=2000)] = 500) -> ActiveExecutorTradesResponse:
        return _read_executor_active_trades(limit=limit)


    @app.get("/api/executor-ledger")
    async def executor_ledger(limit: Annotated[int, Query(ge=1, le=2000)] = 50):
        return _read_executor_ledger(limit=limit)


    @app.get("/api/learning-effectiveness")
    async def learning_effectiveness():
        return _read_learning_effectiveness()


    @app.get("/api/executor-exit-simulator")
    async def executor_exit_simulator():
        return _read_executor_exit_simulator()


    @app.get("/api/signal-profit-potential")
    async def signal_profit_potential():
        return _read_profit_potential_payload()

    @app.get("/api/signal-kind-groups")
    async def signal_kind_groups():
        profit_payload = _read_profit_potential_payload()
        rows = _read_signal_metric_rows()

        if not rows:
            return _empty_signal_kind_groups_payload(profit_payload)
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
        profit_by_kind = _profit_potential_by_kind(profit_payload)
        for row in groups:
            row.profit_potential = profit_by_kind.get(row.kind)
        focus_groups = {focus_group: [] for focus_group in ("HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER")}
        for row in groups:
            focus_groups.setdefault(row.signal_focus_group, []).append(row)
        return {
            "groups": groups,
            "focus_groups": focus_groups,
            "high_potential_focus": _high_potential_focus_payload(rows, profit_payload),
            "profit_potential": profit_payload,
        }

    @app.get("/api/high-potential-focus")
    async def high_potential_focus():
        profit_payload = _read_profit_potential_payload()
        rows = _read_signal_metric_rows()

        if not rows:
            return _empty_high_potential_focus(profit_payload)

        return _high_potential_focus_payload(rows, profit_payload)

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
