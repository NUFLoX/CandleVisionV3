from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field


@dataclass(slots=True)
class TradeTick:
    ts: float
    side: str
    price: float
    size: float

    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass(slots=True)
class WallEvent:
    ts: float
    price: float
    size: float


@dataclass(slots=True)
class SymbolFlowState:
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    trades: deque[TradeTick] = field(default_factory=deque)
    bid_walls: deque[WallEvent] = field(default_factory=deque)
    ask_walls: deque[WallEvent] = field(default_factory=deque)
    last_mid: float | None = None
    last_update_ts: float = 0.0

    def update_book(self, bids: list[list[str]], asks: list[list[str]], is_snapshot: bool) -> None:
        if is_snapshot:
            self.bids = {float(price): float(size) for price, size in bids}
            self.asks = {float(price): float(size) for price, size in asks}
        else:
            for price, size in bids:
                p = float(price)
                s = float(size)
                if s == 0:
                    self.bids.pop(p, None)
                else:
                    self.bids[p] = s
            for price, size in asks:
                p = float(price)
                s = float(size)
                if s == 0:
                    self.asks.pop(p, None)
                else:
                    self.asks[p] = s
        self.last_update_ts = time.time()
        if self.bids and self.asks:
            self.last_mid = (max(self.bids) + min(self.asks)) / 2.0

    def add_trade(self, side: str, price: float, size: float, ts: float | None = None) -> None:
        now = ts or time.time()
        self.trades.append(TradeTick(ts=now, side=side, price=price, size=size))

    def trim(self, trade_window_seconds: int, wall_window_seconds: int) -> None:
        now = time.time()
        while self.trades and now - self.trades[0].ts > trade_window_seconds:
            self.trades.popleft()
        while self.bid_walls and now - self.bid_walls[0].ts > wall_window_seconds:
            self.bid_walls.popleft()
        while self.ask_walls and now - self.ask_walls[0].ts > wall_window_seconds:
            self.ask_walls.popleft()

    def top_book(self, depth: int) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids = sorted(self.bids.items(), key=lambda item: item[0], reverse=True)[:depth]
        asks = sorted(self.asks.items(), key=lambda item: item[0])[:depth]
        return bids, asks
