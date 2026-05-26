from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .schemas import BotLog, Heartbeat, MarketState, Signal, Trade, WatchlistItem
from .signal_outcomes import SignalOutcomeStore, refresh_signal_outcomes
from .store import DashboardStore

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


def create_app() -> FastAPI:
    store = DashboardStore()
    hub = WebSocketHub()
    app = FastAPI(
        title="CandleVision Dashboard API",
        version="0.1.0",
        description="MVP API for bot console, market state, signals, dominance strips, watchlist, trades and coin analytics.",
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

    @app.on_event("startup")
    async def startup() -> None:
        app.state.refresh_task = asyncio.create_task(_live_refresh_loop(store, hub))

    @app.on_event("shutdown")
    async def shutdown() -> None:
        task = app.state.refresh_task
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/status")
    async def status():
        return (await store.snapshot()).status

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

    @app.get("/api/setup-performance")
    async def setup_performance():
        if not SIGNALS_DB_PATH.exists():
            return {"by_reason": [], "by_score_bucket": [], "by_timeframe": [], "by_kind": [], "by_source": []}
            return {"by_reason": [], "by_score_bucket": [], "by_timeframe": []}

        conn = sqlite3.connect(str(SIGNALS_DB_PATH))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT reasons_last, score_last, timeframe, kind, source, status, max_gain_pct, max_drawdown_pct FROM signals"

                "SELECT reasons_last, score_last, timeframe, status, max_gain_pct, max_drawdown_pct FROM signals"

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

        for row in rows:
            outcome = wl(str(row["status"] or ""))
            score = float(row["score_last"] or 0.0)
            tf = str(row["timeframe"] or "1")
            kind = str(row["kind"] or "UNKNOWN") if "kind" in row.keys() else "UNKNOWN"
            source = str(row["source"] or "UNKNOWN") if "source" in row.keys() else "UNKNOWN"
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

            for group in (score_stats[score_b], tf_stats[tf], kind_stats[kind], source_stats[source]):


            for group in (score_stats[score_b], tf_stats[tf], kind_stats[kind], source_stats[source]):

            for group in (score_stats[score_b], tf_stats[tf]):


                group["total"] += 1
                group["mfe"] += mfe
                group["mae"] += mae
                if outcome == "TP":
                    group["tp"] += 1
                elif outcome == "SL":
                    group["sl"] += 1
                else:
                    group["pending"] += 1

        def finalize(items: dict, label: str) -> list[dict]:
            out = []
            for key, value in items.items():
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
            return sorted(out, key=lambda row: row["total"], reverse=True)

        return {
            "by_reason": finalize(reason_stats, "reason"),
            "by_score_bucket": finalize(score_stats, "score_bucket"),
            "by_timeframe": finalize(tf_stats, "timeframe"),
            "by_kind": finalize(kind_stats, "kind"),
            "by_source": finalize(source_stats, "source"),
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
