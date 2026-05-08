from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from .schemas import (
    BotLog,
    BotStatus,
    CoinMetrics,
    DashboardSnapshot,
    MarketState,
    PressureStrip,
    Signal,
    SignalStrength,
    SignalType,
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
        self.status = BotStatus()
        self.market_state = MarketState()
        self.pressure_strips = _demo_pressure_strips()
        self.signals: deque[Signal] = deque(_demo_signals(), maxlen=300)
        self.logs: deque[BotLog] = deque(_demo_logs(), maxlen=500)
        self.watchlist: deque[WatchlistItem] = deque(_demo_watchlist(), maxlen=200)
        self.trades: deque[Trade] = deque(_demo_trades(), maxlen=300)
        self.coin_metrics: dict[str, CoinMetrics] = _demo_coin_metrics()

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
            if symbol in self.coin_metrics:
                return deepcopy(self.coin_metrics[symbol])
            base = self.coin_metrics["API3USDT"].copy(deep=True)
            base.symbol = symbol
            base.market_cap_usd = 0
            base.volume_24h_usd = 0
            base.money_inflow_1h_usd = 0
            base.money_inflow_4h_usd = 0
            base.money_inflow_24h_usd = 0
            base.bot_verdict = "No live metrics yet. Add this symbol to the scanner/watchlist to populate analytics."
            base.updated_at = _now()
            return base

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


def normalize_symbol(symbol: str) -> str:
    value = symbol.strip().upper().replace("/", "")
    if not value.endswith("USDT") and value not in {"BTC", "ETH"}:
        value = f"{value}USDT"
    if value == "BTC":
        value = "BTCUSDT"
    if value == "ETH":
        value = "ETHUSDT"
    return value


def _demo_logs() -> list[BotLog]:
    return [
        BotLog(timestamp=_now(), message="BTC filter: stable, alt signals allowed", source="market", severity="success"),
        BotLog(timestamp=_now(), message="API3USDT: score 8.7, confirmed breakout", source="scanner", severity="success"),
        BotLog(timestamp=_now(), message="Bybit rate-limit protection: OK", source="gateway", severity="info"),
        BotLog(timestamp=_now(), message="Telegram delivery: sent", source="notifier", severity="success"),
        BotLog(timestamp=_now(), message="Executor status: online, 2 open trades", source="executor", severity="info"),
    ]


def _demo_signals() -> list[Signal]:
    return [
        Signal(
            id=str(uuid4()),
            symbol="API3USDT",
            exchange="Bybit",
            timeframe="1h",
            score=8.7,
            strength=SignalStrength.strong,
            signal_type=SignalType.confirmed,
            entry=0.912,
            stop_loss=0.848,
            take_profit_1=1.04,
            take_profit_2=1.12,
            reason="EMA20 up + VSpike q95 + breakout",
        ),
        Signal(
            id=str(uuid4()),
            symbol="SOLUSDT",
            exchange="Binance",
            timeframe="4h",
            score=7.4,
            strength=SignalStrength.medium,
            signal_type=SignalType.watchlist,
            entry=154.2,
            stop_loss=146.8,
            take_profit_1=168.5,
            reason="TOTAL3 strength + support reclaim + volume expansion",
            status="WATCHING",
        ),
        Signal(
            id=str(uuid4()),
            symbol="ETHUSDT",
            exchange="Bybit",
            timeframe="1d",
            score=6.3,
            strength=SignalStrength.weak,
            signal_type=SignalType.aggressive,
            entry=3064.0,
            stop_loss=2922.0,
            take_profit_1=3310.0,
            reason="Aggressive retest, liquidity medium",
        ),
    ]


def _demo_pressure_strips() -> list[PressureStrip]:
    return [
        PressureStrip(key="btc_cap", label="BTC Market Cap", value=78, change_pct=1.8, direction="up", interpretation="Capital flows into BTC"),
        PressureStrip(key="btc_d", label="BTC Dominance", value=58, change_pct=0.4, direction="up", interpretation="BTC is leading the tape"),
        PressureStrip(key="usdt_d", label="USDT Dominance", value=28, change_pct=-0.7, direction="down", interpretation="Stablecoin pressure is fading"),
        PressureStrip(key="total3", label="TOTAL3", value=84, change_pct=2.6, direction="up", interpretation="Altcoins are waking up"),
    ]


def _demo_watchlist() -> list[WatchlistItem]:
    return [
        WatchlistItem(symbol="NEARUSDT", exchange="Bybit", timeframe="4h", score=7.1, reason="Compression below resistance, waiting breakout", expires_in_hours=36),
        WatchlistItem(symbol="SUIUSDT", exchange="Binance", timeframe="1h", score=6.9, reason="Orderbook support improving", expires_in_hours=18),
    ]


def _demo_trades() -> list[Trade]:
    return [
        Trade(id=1, symbol="API3USDT", timeframe="1h", entry=0.912, stop_loss=0.848, take_profit=1.04, status="open", pnl_pct=3.2),
        Trade(id=2, symbol="DOGEUSDT", timeframe="4h", entry=0.144, stop_loss=0.137, take_profit=0.161, status="closed", pnl_pct=5.7),
    ]


def _demo_coin_metrics() -> dict[str, CoinMetrics]:
    api3 = CoinMetrics(
        symbol="API3USDT",
        market_cap_usd=109_000_000,
        volume_24h_usd=18_600_000,
        money_inflow_1h_usd=740_000,
        money_inflow_4h_usd=3_200_000,
        money_inflow_24h_usd=8_900_000,
        cex_netflow_usd=-410_000,
        whale_activity="Elevated",
        accumulation_score=8.7,
        orderbook_imbalance=0.62,
        rsi=63.4,
        atr_pct=5.8,
        ema20=0.901,
        ema50=0.874,
        ema200=0.792,
        volume_spike="q95",
        support=0.884,
        resistance=1.04,
        bot_verdict="High momentum. Breakout confirmed. Volume spike q95. Orderbook support detected.",
    )
    btc = api3.copy(deep=True)
    btc.symbol = "BTCUSDT"
    btc.market_cap_usd = 1_420_000_000_000
    btc.volume_24h_usd = 42_000_000_000
    btc.accumulation_score = 7.9
    btc.bot_verdict = "BTC is stable and not blocking alt signals. Watch dominance acceleration."
    eth = api3.copy(deep=True)
    eth.symbol = "ETHUSDT"
    eth.market_cap_usd = 385_000_000_000
    eth.volume_24h_usd = 21_000_000_000
    eth.accumulation_score = 7.2
    eth.bot_verdict = "Constructive structure, but confirmation depends on TOTAL3 continuation."
    return {item.symbol: item for item in (api3, btc, eth)}
