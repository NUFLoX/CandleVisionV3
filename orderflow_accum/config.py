
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _csv_env(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


@dataclass(slots=True)
class Settings:
    bybit_testnet: bool = os.getenv("BYBIT_TESTNET", "false").lower() == "true"
    signals_only: bool = os.getenv("SIGNALS_ONLY", "true").lower() == "true"
    quote_coin: str = os.getenv("ACC_QUOTE_COIN", "USDT").upper()

    signal_mode: str = os.getenv("ACC_SIGNAL_MODE", "CONFIRMED_MODE").strip().upper()


    realtime_symbols_limit: int = int(os.getenv("ACC_REALTIME_SYMBOLS_LIMIT", "20"))
    macro_symbols_limit: int = int(os.getenv("ACC_MACRO_SYMBOLS_LIMIT", "50"))
    realtime_scan_every_seconds: int = int(os.getenv("ACC_SCAN_EVERY_SECONDS", "12"))
    realtime_intervals: list[str] = field(default_factory=lambda: _csv_env("ACC_REALTIME_INTERVALS", "1,5,15"))

    preimpulse_intervals: list[str] = field(default_factory=lambda: _csv_env("ACC_PREIMPULSE_INTERVALS", "5,15,60"))
    market_categories: list[str] = field(default_factory=lambda: _csv_env("ACC_MARKET_CATEGORIES", "LINEAR"))
    market_categories: list[str] = field(default_factory=lambda: _csv_env("ACC_MARKET_CATEGORIES", "LINEAR"))


    macro_every_seconds: int = int(os.getenv("ACC_MACRO_EVERY_SECONDS", "1800"))
    book_depth: int = int(os.getenv("ACC_BOOK_DEPTH", "20"))

    min_notional_24h: float = float(os.getenv("ACC_MIN_NOTIONAL_24H", "15000000"))
    min_last_price: float = float(os.getenv("ACC_MIN_LAST_PRICE", "0.000001"))

    support_lookback_bars: int = int(os.getenv("ACC_SUPPORT_LOOKBACK_BARS", "60"))
    resistance_lookback_bars: int = int(os.getenv("ACC_RESISTANCE_LOOKBACK_BARS", "60"))
    support_tolerance_bps: float = float(os.getenv("ACC_SUPPORT_TOLERANCE_BPS", "22"))
    breakout_ready_bps: float = float(os.getenv("ACC_BREAKOUT_READY_BPS", "55"))

    tape_window_seconds: int = int(os.getenv("ACC_TAPE_WINDOW_SECONDS", "18"))
    wall_persistence_seconds: int = int(os.getenv("ACC_WALL_PERSISTENCE_SECONDS", "18"))
    ws_heartbeat_seconds: int = int(os.getenv("ACC_WS_HEARTBEAT_SECONDS", "20"))
    stale_book_seconds: int = int(os.getenv("ACC_STALE_BOOK_SECONDS", "12"))
    signal_cooldown_seconds: int = int(os.getenv("ACC_SIGNAL_COOLDOWN_SECONDS", "1800"))

    fixed_risk_pct: float = float(os.getenv("ACC_FIXED_RISK_PCT", "0.35"))
    resistance_buffer_bps: float = float(os.getenv("ACC_RESISTANCE_BUFFER_BPS", "12"))
    second_resistance_buffer_bps: float = float(os.getenv("ACC_SECOND_RESISTANCE_BUFFER_BPS", "18"))
    min_reward_to_r1: float = float(os.getenv("ACC_MIN_REWARD_TO_R1", "0.75"))
    min_resistance_gap_bps: float = float(os.getenv("ACC_MIN_RESISTANCE_GAP_BPS", "10"))
    atr_stop_mult: float = float(os.getenv("ACC_ATR_STOP_MULT", "1.25"))
    tp1_rr: float = float(os.getenv("ACC_TP1_RR", "1.6"))
    tp2_rr: float = float(os.getenv("ACC_TP2_RR", "2.8"))

    min_trade_notional: float = float(os.getenv("ACC_MIN_TRADE_NOTIONAL", "30000"))
    min_sell_pressure_notional: float = float(os.getenv("ACC_MIN_SELL_PRESSURE_NOTIONAL", "12000"))
    min_wall_persistence: float = float(os.getenv("ACC_MIN_WALL_PERSISTENCE", "0.18"))
    min_book_imbalance: float = float(os.getenv("ACC_MIN_BOOK_IMBALANCE", "0.08"))
    max_spread_bps: float = float(os.getenv("ACC_MAX_SPREAD_BPS", "10"))
    min_absorption_score: float = float(os.getenv("ACC_MIN_ABSORPTION_SCORE", "3.6"))
    min_ready_score: float = float(os.getenv("ACC_MIN_READY_SCORE", "4.8"))
    min_macro_score: float = float(os.getenv("ACC_MIN_MACRO_SCORE", "3.8"))

    macro_base_lookback: int = int(os.getenv("ACC_MACRO_BASE_LOOKBACK", "24"))
    macro_max_range_pct_60: float = float(os.getenv("ACC_MACRO_MAX_RANGE_PCT_60", "6.0"))
    macro_max_range_pct_240: float = float(os.getenv("ACC_MACRO_MAX_RANGE_PCT_240", "9.0"))
    macro_max_range_pct_d: float = float(os.getenv("ACC_MACRO_MAX_RANGE_PCT_D", "18.0"))
    macro_min_turnover_60: float = float(os.getenv("ACC_MACRO_MIN_TURNOVER_60", "40000000"))
    macro_min_turnover_240: float = float(os.getenv("ACC_MACRO_MIN_TURNOVER_240", "100000000"))
    macro_min_turnover_d: float = float(os.getenv("ACC_MACRO_MIN_TURNOVER_D", "250000000"))
    macro_min_close_pos: float = float(os.getenv("ACC_MACRO_MIN_CLOSE_POS", "0.62"))
    macro_max_recent_impulse_pct: float = float(os.getenv("ACC_MACRO_MAX_RECENT_IMPULSE_PCT", "7.5"))
    macro_min_volume_expansion: float = float(os.getenv("ACC_MACRO_MIN_VOLUME_EXPANSION", "1.2"))
    macro_symbol_cooldown_minutes: int = int(os.getenv("ACC_MACRO_SYMBOL_COOLDOWN_MIN", "360"))

    symbols_allowlist: list[str] = field(default_factory=lambda: _csv_env("ACC_SYMBOLS_ALLOWLIST"))
    symbols_blocklist: list[str] = field(default_factory=lambda: _csv_env("ACC_SYMBOLS_BLOCKLIST", "XAUTUSDT"))
    symbol_exclude_patterns: list[str] = field(default_factory=lambda: _csv_env("ACC_SYMBOL_EXCLUDE_PATTERNS", "1000*,*TEST*,*DEMO*,*USDC"))


    telegram_send_charts: bool = os.getenv("ACC_TELEGRAM_SEND_CHARTS", "true").lower() == "true"
    chart_bars_macro: int = int(os.getenv("ACC_CHART_BARS_MACRO", "80"))
    chart_bars_realtime: int = int(os.getenv("ACC_CHART_BARS_REALTIME", "120"))

    rest_timeout_seconds: int = int(os.getenv("ACC_REST_TIMEOUT_SECONDS", "25"))
    rest_retries: int = int(os.getenv("ACC_REST_RETRIES", "2"))

    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "").strip()


    @property
    def is_early_mode(self) -> bool:
        return self.signal_mode == "EARLY_MODE"

    @property
    def is_confirmed_mode(self) -> bool:
        return self.signal_mode != "EARLY_MODE"

    @property
    def effective_min_reward_to_r1(self) -> float:
        return self.min_reward_to_r1 * (0.78 if self.is_early_mode else 1.08)

    @property
    def effective_min_absorption_score(self) -> float:
        return self.min_absorption_score * (0.82 if self.is_early_mode else 1.05)

    @property
    def effective_min_ready_score(self) -> float:
        return self.min_ready_score * (0.90 if self.is_early_mode else 1.06)

    @property
    def effective_min_macro_score(self) -> float:
        return self.min_macro_score * (0.88 if self.is_early_mode else 1.08)

    @property
    def effective_min_trade_notional(self) -> float:
        return self.min_trade_notional * (0.78 if self.is_early_mode else 1.0)

    @property
    def effective_min_sell_pressure_notional(self) -> float:
        return self.min_sell_pressure_notional * (0.76 if self.is_early_mode else 1.0)

    @property
    def effective_min_wall_persistence(self) -> float:
        return self.min_wall_persistence * (0.82 if self.is_early_mode else 1.05)

    @property
    def effective_min_book_imbalance(self) -> float:
        return self.min_book_imbalance * (0.78 if self.is_early_mode else 1.05)

    @property
    def effective_breakout_ready_bps(self) -> float:
        return self.breakout_ready_bps * (1.35 if self.is_early_mode else 0.92)

    @property
    def effective_macro_min_close_pos(self) -> float:
        return self.macro_min_close_pos * (0.92 if self.is_early_mode else 1.02)

    @property
    def effective_macro_min_volume_expansion(self) -> float:
        return self.macro_min_volume_expansion * (0.92 if self.is_early_mode else 1.05)


    @property
    def rest_base_url(self) -> str:
        return "https://api-testnet.bybit.com" if self.bybit_testnet else "https://api.bybit.com"

    @property
    def ws_public_url(self) -> str:
        return "wss://stream-testnet.bybit.com/v5/public/linear" if self.bybit_testnet else "wss://stream.bybit.com/v5/public/linear"
