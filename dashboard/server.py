from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .schemas import BotLog, MarketState, Signal
from .store import DashboardStore

STATIC_DIR = Path(__file__).resolve().parent / "static"


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


def _jsonable(payload: object) -> object:
    return jsonable_encoder(payload)


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

    @app.get("/api/watchlist")
    async def watchlist():
        return (await store.snapshot()).watchlist

    @app.get("/api/trades")
    async def trades():
        return (await store.snapshot()).trades

    @app.get("/api/coin/{symbol}")
    async def coin(symbol: str):
        return await store.coin(symbol)

    @app.get("/api/snapshot")
    async def snapshot():
        return await store.snapshot()

    @app.post("/api/ingest/log")
    async def ingest_log(log: BotLog):
        saved = await store.add_log(log)
        await hub.broadcast("log", saved)
        return saved

    @app.post("/api/ingest/signal")
    async def ingest_signal(signal: Signal):
        saved = await store.add_signal(signal)
        await hub.broadcast("signal", saved)
        return saved

    @app.post("/api/ingest/market-state")
    async def ingest_market_state(state: MarketState):
        saved = await store.update_market_state(state)
        await hub.broadcast("market-state", saved)
        return saved

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
