from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Severity(str, Enum):
    info = "info"
    warning = "warning"
    error = "error"
    success = "success"


class SignalStrength(str, Enum):
    strong = "Strong"
    medium = "Medium"
    weak = "Weak"


class SignalType(str, Enum):
    watchlist = "Watchlist"
    confirmed = "Confirmed"
    aggressive = "Aggressive"


class BotLog(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message: str
    source: str = "scanner"
    severity: Severity = Severity.info


class BotStatus(BaseModel):
    scanner: str = "online"
    executor: str = "online"
    telegram: str = "online"
    x_delivery: str = "standby"
    bybit_api: str = "OK"
    binance_api: str = "OK"
    database: str = "OK"
    redis: str = "OK"
    rate_limit: str = "protected"
    last_scan_seconds: int = 12
    open_trades: int = 2
    closed_trades_today: int = 4


class MarketState(BaseModel):
    btc_filter: str = "STABLE"
    altcoin_mode: str = "RISK-ON"
    liquidity: str = "HIGH"
    market_regime: str = "BULL"
    usdt_dominance_trend: str = "falling"
    total3_strength: str = "strong"
    can_emit_alt_signals: bool = True
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PressureStrip(BaseModel):
    key: str
    label: str
    value: float = Field(ge=0, le=100)
    change_pct: float
    direction: str
    interpretation: str


class Signal(BaseModel):
    id: str
    symbol: str
    exchange: str
    timeframe: str
    score: float
    strength: SignalStrength
    signal_type: SignalType
    entry: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float | None = None
    reason: str
    status: str = "ACTIVE"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class WatchlistItem(BaseModel):
    symbol: str
    exchange: str
    timeframe: str
    score: float
    reason: str
    expires_in_hours: int = 24


class CoinMetrics(BaseModel):
    symbol: str
    market_cap_usd: float
    volume_24h_usd: float
    money_inflow_1h_usd: float
    money_inflow_4h_usd: float
    money_inflow_24h_usd: float
    cex_netflow_usd: float
    whale_activity: str
    accumulation_score: float
    orderbook_imbalance: float
    rsi: float
    atr_pct: float
    ema20: float
    ema50: float
    ema200: float
    volume_spike: str
    support: float
    resistance: float
    bot_verdict: str
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Trade(BaseModel):
    id: int
    symbol: str
    timeframe: str
    entry: float
    stop_loss: float
    take_profit: float
    status: str
    pnl_pct: float


class DashboardSnapshot(BaseModel):
    status: BotStatus
    market_state: MarketState
    pressure_strips: list[PressureStrip]
    signals: list[Signal]
    logs: list[BotLog]
    watchlist: list[WatchlistItem]
    trades: list[Trade]
    meta: dict[str, Any] = Field(default_factory=dict)
