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
    message: str = ""
    source: str = "scanner"
    severity: Severity = Severity.info


class BotStatus(BaseModel):
    scanner: str = "standby"
    executor: str = "standby"
    telegram: str = "not-checked"
    x_delivery: str = "standby"
    bybit_api: str = "not-checked"
    binance_api: str = "not-configured"
    database: str = "not-checked"
    redis: str = "not-checked"
    rate_limit: str = "not-checked"
    last_scan_seconds: int = 0
    open_trades: int = 0
    closed_trades_today: int = 0


class MarketState(BaseModel):
    btc_filter: str = "UNKNOWN"
    altcoin_mode: str = "WAITING"
    liquidity: str = "UNKNOWN"
    market_regime: str = "UNKNOWN"
    usdt_dominance_trend: str = "unknown"
    total3_strength: str = "unknown"
    can_emit_alt_signals: bool = False
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PressureStrip(BaseModel):
    key: str = "unknown"
    label: str = "Unknown"
    value: float = Field(default=0.0, ge=0, le=100)
    change_pct: float = 0.0
    direction: str = "flat"
    interpretation: str = "No live data yet"


class Signal(BaseModel):
    id: str = ""
    symbol: str = "UNKNOWN"
    exchange: str = "Bybit"
    timeframe: str = "1h"
    score: float = 0.0
    strength: SignalStrength = SignalStrength.weak
    signal_type: SignalType = SignalType.watchlist
    entry: float = 0.0
    stop_loss: float = 0.0
    take_profit_1: float = 0.0
    take_profit_2: float | None = None
    reason: str = ""
    status: str = "ACTIVE"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    signal_kind: str = "SIGNAL"
    signal_family: str = "unclassified"
    signal_focus_group: str = "unclassified"
    signal_source: str = "scanner"
    signal_timeframe: str = "1h"


class SignalKindGroupStats(BaseModel):
    kind: str = "OTHER"
    signal_family: str = "OTHER"
    signal_focus_group: str = "OTHER"
    timeframe: str = "UNKNOWN"
    source: str = "UNKNOWN"
    total: int = 0
    tp2: int = 0
    sl: int = 0
    expired: int = 0
    confirmed: int = 0
    tp2_rate_closed_pct: float = 0.0
    avg_score_last: float = 0.0
    avg_score_max: float = 0.0
    avg_max_gain_pct: float = 0.0
    avg_max_drawdown_pct: float = 0.0


class WatchlistItem(BaseModel):
    symbol: str = "UNKNOWN"
    exchange: str = "Bybit"
    timeframe: str = "1h"
    score: float = 0.0
    reason: str = ""
    expires_in_hours: int = 24


class CoinMetrics(BaseModel):
    symbol: str = "UNKNOWN"
    market_cap_usd: float = 0.0
    volume_24h_usd: float = 0.0
    money_inflow_1h_usd: float = 0.0
    money_inflow_4h_usd: float = 0.0
    money_inflow_24h_usd: float = 0.0
    cex_netflow_usd: float = 0.0
    whale_activity: str = "unavailable"
    accumulation_score: float = 0.0
    orderbook_imbalance: float = 0.0
    rsi: float = 0.0
    atr_pct: float = 0.0
    ema20: float = 0.0
    ema50: float = 0.0
    ema200: float = 0.0
    volume_spike: str = "unknown"
    support: float = 0.0
    resistance: float = 0.0
    bot_verdict: str = "No live data yet"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Trade(BaseModel):
    id: int | None = None
    symbol: str = "UNKNOWN"
    timeframe: str = "1h"
    entry: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    status: str = "unknown"
    pnl_pct: float = 0.0


class Heartbeat(BaseModel):
    component: str = "unknown"
    status: str = "online"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    meta: dict[str, Any] = Field(default_factory=dict)


class DashboardSnapshot(BaseModel):
    status: BotStatus = Field(default_factory=BotStatus)
    market_state: MarketState = Field(default_factory=MarketState)
    pressure_strips: list[PressureStrip] = Field(default_factory=list)
    signals: list[Signal] = Field(default_factory=list)
    logs: list[BotLog] = Field(default_factory=list)
    watchlist: list[WatchlistItem] = Field(default_factory=list)
    trades: list[Trade] = Field(default_factory=list)
    heartbeats: dict[str, Heartbeat] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)



class SignalOutcome(BaseModel):
    signal_id: str = ""
    symbol: str = "UNKNOWN"
    exchange: str = "Bybit"
    timeframe: str = "1h"
    signal_type: str = "Watchlist"
    direction: str = "long"
    entry: float = 0.0
    stop_loss: float = 0.0
    take_profit_1: float = 0.0
    outcome: str = "expired"
    r_multiple: float = 0.0
    bars_checked: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SignalStatsSummary(BaseModel):
    total: int = 0
    wins: int = 0
    losses: int = 0
    ambiguous: int = 0
    expired: int = 0
    win_rate: float = 0.0
    avg_r: float = 0.0
    expectancy_r: float = 0.0
    total_r: float = 0.0
    by_symbol: list[dict[str, Any]] = Field(default_factory=list)
    by_timeframe: list[dict[str, Any]] = Field(default_factory=list)
    by_signal_type: list[dict[str, Any]] = Field(default_factory=list)
    by_outcome: list[dict[str, Any]] = Field(default_factory=list)
