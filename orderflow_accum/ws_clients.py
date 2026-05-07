
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections import defaultdict

import websockets

from .bookflow import SymbolFlowState, WallEvent

logger = logging.getLogger("Accum.WS")


class MarketStream:
    def __init__(
        self,
        url: str,
        book_depth: int = 20,
        tape_window_seconds: int = 18,
        wall_persistence_seconds: int = 18,
        heartbeat_seconds: int = 20,
    ):
        self.url = url
        self.book_depth = book_depth
        self.tape_window_seconds = tape_window_seconds
        self.wall_persistence_seconds = wall_persistence_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.state: dict[str, SymbolFlowState] = defaultdict(SymbolFlowState)
        self._ws = None
        self.status = "BOOT"

    async def run(self, symbols: list[str]) -> None:
        retry_delay = 3
        while True:
            heartbeat_task: asyncio.Task | None = None
            try:
                logger.info("Connecting WS for %s symbols", len(symbols))
                self.status = "CONNECTING"
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=10,
                    open_timeout=20,
                    max_size=2**24,
                    max_queue=4096,
                ) as ws:
                    self._ws = ws
                    await self._subscribe(symbols)
                    self.status = "LIVE"
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws), name="accum_ws_heartbeat")
                    retry_delay = 3

                    async for message in ws:
                        self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.status = "RECONNECTING"
                logger.warning("WS disconnected: %s", exc)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
            finally:
                self._ws = None
                if heartbeat_task:
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await heartbeat_task

    async def _heartbeat_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:
                logger.debug("WS heartbeat stopped", exc_info=True)
                return

    async def _subscribe(self, symbols: list[str]) -> None:
        if not self._ws:
            return
        batch_size = 10
        for idx in range(0, len(symbols), batch_size):
            batch = symbols[idx: idx + batch_size]
            args = [f"orderbook.50.{symbol}" for symbol in batch]
            args.extend([f"publicTrade.{symbol}" for symbol in batch])
            await self._ws.send(json.dumps({"op": "subscribe", "args": args}))
            await asyncio.sleep(0.25)

    def _handle_message(self, raw: str) -> None:
        payload = json.loads(raw)
        topic = payload.get("topic", "")
        op = payload.get("op", "")

        if op in {"ping", "pong"}:
            return
        if payload.get("success") is True and payload.get("op") == "subscribe":
            return

        if topic.startswith("orderbook"):
            self._handle_orderbook(payload)
        elif topic.startswith("publicTrade"):
            self._handle_trade(payload)

    def _handle_orderbook(self, payload: dict) -> None:
        data = payload.get("data", {})
        symbol = data.get("s")
        if not symbol:
            return
        state = self.state[symbol]
        state.update_book(data.get("b", []), data.get("a", []), payload.get("type") == "snapshot")
        bids, asks = state.top_book(self.book_depth)
        if bids:
            bid_sizes = [size for _, size in bids]
            threshold = max(sum(bid_sizes) / max(len(bid_sizes), 1) * 2.5, 1.0)
            for price, size in bids[:6]:
                if size >= threshold:
                    state.bid_walls.append(WallEvent(ts=time.time(), price=price, size=size))
        if asks:
            ask_sizes = [size for _, size in asks]
            threshold = max(sum(ask_sizes) / max(len(ask_sizes), 1) * 2.5, 1.0)
            for price, size in asks[:6]:
                if size >= threshold:
                    state.ask_walls.append(WallEvent(ts=time.time(), price=price, size=size))
        state.trim(self.tape_window_seconds, self.wall_persistence_seconds)

    def _handle_trade(self, payload: dict) -> None:
        for trade in payload.get("data", []):
            symbol = trade.get("s")
            if not symbol:
                continue
            state = self.state[symbol]
            state.add_trade(
                side=trade.get("S", ""),
                price=float(trade.get("p", 0.0)),
                size=float(trade.get("v", 0.0)),
                ts=float(trade.get("T", 0.0)) / 1000.0 if trade.get("T") else time.time(),
            )
            state.trim(self.tape_window_seconds, self.wall_persistence_seconds)

    def get_state(self, symbol: str) -> SymbolFlowState | None:
        return self.state.get(symbol)
