from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from .schemas import (
    BotLog,
    BotStatus,
    CoinMetrics,
    DashboardSnapshot,
    MarketState,
    PressureStrip,
    Signal,
    Trade,
    WatchlistItem,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DashboardStore:
    """Small live data hub for the MVP dashboard.

    The bot can push events into this store through the ingest endpoints. The
    implementation is intentionally in-memory for first-run simplicity; it keeps
    the public API stable so a Redis/PostgreSQL-backed store can replace it
    without changing the dashboard frontend.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.status = BotStatus(
            scanner="standby",
            executor="standby",
            telegram="configured-by-env",
            x_delivery="standby",
            bybit_api="not-checked",
            binance_api="not-configured",
            database="in-memory",
            redis="not-configured",
            rate_limit="not-checked",
            last_scan_seconds=0,
            open_trades=0,
            closed_trades_today=0,
        )
        self.market_state = MarketState(
            btc_filter="UNKNOWN",
            altcoin_mode="WAITING",
            liquidity="UNKNOWN",
            market_regime="UNKNOWN",
            usdt_dominance_trend="unknown",
            total3_strength="unknown",
            can_emit_alt_signals=False,
        )
        self.pressure_strips: list[PressureStrip] = []
        self.signals: deque[Signal] = deque(maxlen=300)
        self.logs: deque[BotLog] = deque(
            [BotLog(timestamp=_now(), message="Dashboard started without demo data; waiting for live feeds or bot ingest.", source="dashboard", severity="info")],
            maxlen=500,
        )
        self.watchlist: deque[WatchlistItem] = deque(maxlen=200)
        self.trades: deque[Trade] = deque(maxlen=300)
        self.coin_metrics: dict[str, CoinMetrics] = {}

    async def snapshot(self) -> DashboardSnapshot:
        async with self._lock:
            return DashboardSnapshot(
                status=deepcopy(self.status),
                market_state=deepcopy(self.market_state),
                pressure_strips=list(deepcopy(self.pressure_strips)),
                signals=list(deepcopy(self.signals)),
                logs=list(deepcopy(self.logs)),
                watchlist=list(deepcopy(self.watchlist)),
                trades=list(deepcopy(self.trades)),
                meta={"mode": "mvp-in-memory", "updated_at": _now().isoformat()},
            )

    async def list_signals(
        self,
        strength: str | None = None,
        signal_type: str | None = None,
        exchange: str | None = None,
        timeframe: str | None = None,
    ) -> list[Signal]:
        snapshot = await self.snapshot()
        signals = snapshot.signals
        if strength:
            signals = [s for s in signals if s.strength.value.lower() == strength.lower()]
        if signal_type:
            signals = [s for s in signals if s.signal_type.value.lower() == signal_type.lower()]
        if exchange:
            signals = [s for s in signals if s.exchange.lower() == exchange.lower()]
        if timeframe:
            signals = [s for s in signals if s.timeframe.lower() == timeframe.lower()]
        return signals

    async def coin(self, raw_symbol: str) -> CoinMetrics:
        symbol = normalize_symbol(raw_symbol)
        async with self._lock:
            cached = deepcopy(self.coin_metrics.get(symbol))
        if cached:
            return cached

        from .live_data import fetch_live_coin_metrics

        metric = await fetch_live_coin_metrics(symbol)
        async with self._lock:
            self.coin_metrics[symbol] = metric
        return deepcopy(metric)

    async def add_log(self, log: BotLog) -> BotLog:
        async with self._lock:
            self.logs.appendleft(log)
        return log

    async def add_signal(self, signal: Signal) -> Signal:
        async with self._lock:
            self.signals.appendleft(signal)
            self.status.last_scan_seconds = 0
        return signal

    async def update_market_state(self, market_state: MarketState) -> MarketState:
        async with self._lock:
            market_state.updated_at = _now()
            self.market_state = market_state
        return market_state

    async def refresh_live_data(self) -> None:
        from .live_data import fetch_live_dashboard_data

        live_data = await fetch_live_dashboard_data()
        async with self._lock:
            self.status = live_data.status
            self.market_state = live_data.market_state
            self.pressure_strips = live_data.pressure_strips
            self.coin_metrics.update(live_data.coin_metrics)
            for log in reversed(live_data.logs):
                self.logs.appendleft(log)


def normalize_symbol(symbol: str) -> str:
    value = symbol.strip().upper().replace("/", "")
    if not value.endswith("USDT") and value not in {"BTC", "ETH"}:
        value = f"{value}USDT"
    if value == "BTC":
        value = "BTCUSDT"
    if value == "ETH":
        value = "ETHUSDT"
    return value
