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
    quote_coin: str = os.getenv("OF_QUOTE_COIN", "USDT").upper()

    realtime_symbols_limit: int = int(os.getenv("OF_REALTIME_SYMBOLS_LIMIT", "40"))
    macro_symbols_limit: int = int(os.getenv("OF_MACRO_SYMBOLS_LIMIT", "95"))
    scan_every_seconds: int = int(os.getenv("OF_SCAN_EVERY_SECONDS", "20"))
    macro_every_seconds: int = int(os.getenv("OF_MACRO_EVERY_SECONDS", "7200"))
    book_depth: int = int(os.getenv("OF_BOOK_DEPTH", "25"))

    min_notional_24h: float = float(os.getenv("OF_MIN_NOTIONAL_24H", "10000000"))
    min_last_price: float = float(os.getenv("OF_MIN_LAST_PRICE", "0.000001"))

    support_lookback_bars: int = int(os.getenv("OF_SUPPORT_LOOKBACK_BARS", "60"))
    support_tolerance_bps: float = float(os.getenv("OF_SUPPORT_TOLERANCE_BPS", "18"))
    resistance_tolerance_bps: float = float(os.getenv("OF_RESISTANCE_TOLERANCE_BPS", "18"))
    min_absorption_score: float = float(os.getenv("OF_MIN_ABSORPTION_SCORE", "4.0"))
    min_breakout_score: float = float(os.getenv("OF_MIN_BREAKOUT_SCORE", "4.2"))
    min_macro_score: float = float(os.getenv("OF_MIN_MACRO_SCORE", "3.4"))

    tape_window_seconds: int = int(os.getenv("OF_TAPE_WINDOW_SECONDS", "15"))
    wall_persistence_seconds: int = int(os.getenv("OF_WALL_PERSISTENCE_SECONDS", "12"))
    ws_heartbeat_seconds: int = int(os.getenv("OF_WS_HEARTBEAT_SECONDS", "20"))
    stale_book_seconds: int = int(os.getenv("OF_STALE_BOOK_SECONDS", "6"))
    signal_cooldown_seconds: int = int(os.getenv("OF_SIGNAL_COOLDOWN_SECONDS", "300"))
    macro_symbol_cooldown_minutes: int = int(os.getenv("OF_MACRO_SYMBOL_COOLDOWN_MIN", "240"))

    fixed_risk_pct: float = float(os.getenv("OF_FIXED_RISK_PCT", "0.35"))
    atr_stop_mult: float = float(os.getenv("OF_ATR_STOP_MULT", "1.2"))
    tp1_rr: float = float(os.getenv("OF_TP1_RR", "1.5"))
    tp2_rr: float = float(os.getenv("OF_TP2_RR", "2.5"))

    min_realtime_trade_notional: float = float(os.getenv("OF_MIN_REALTIME_TRADE_NOTIONAL", "50000"))
    min_delta_abs: float = float(os.getenv("OF_MIN_DELTA_ABS", "15000"))
    min_wall_persistence: float = float(os.getenv("OF_MIN_WALL_PERSISTENCE", "0.22"))
    min_book_imbalance_abs: float = float(os.getenv("OF_MIN_BOOK_IMBALANCE_ABS", "0.14"))
    max_spread_bps: float = float(os.getenv("OF_MAX_SPREAD_BPS", "8"))
    min_breakout_turnover_multiple: float = float(os.getenv("OF_MIN_BREAKOUT_TURNOVER_MULTIPLE", "1.5"))

    macro_require_breakout_proximity: bool = os.getenv("OF_MACRO_REQUIRE_BREAKOUT_PROXIMITY", "true").lower() == "true"
    macro_max_close_to_breakout_pct: float = float(os.getenv("OF_MACRO_MAX_CLOSE_TO_BREAKOUT_PCT", "2.2"))
    macro_min_volume_expansion: float = float(os.getenv("OF_MACRO_MIN_VOLUME_EXPANSION", "1.8"))
    macro_min_turnover_60: float = float(os.getenv("OF_MACRO_MIN_TURNOVER_60", "50000000"))
    macro_min_turnover_240: float = float(os.getenv("OF_MACRO_MIN_TURNOVER_240", "120000000"))
    macro_min_turnover_d: float = float(os.getenv("OF_MACRO_MIN_TURNOVER_D", "350000000"))
    macro_require_trend_alignment: bool = os.getenv("OF_MACRO_REQUIRE_TREND_ALIGNMENT", "true").lower() == "true"
    macro_max_corridor_pct_60: float = float(os.getenv("OF_MACRO_MAX_CORRIDOR_PCT_60", "4.0"))
    macro_max_corridor_pct_240: float = float(os.getenv("OF_MACRO_MAX_CORRIDOR_PCT_240", "7.0"))
    macro_max_corridor_pct_d: float = float(os.getenv("OF_MACRO_MAX_CORRIDOR_PCT_D", "16.0"))

    symbols_allowlist: list[str] = field(default_factory=lambda: _csv_env("OF_SYMBOLS_ALLOWLIST"))
    symbols_blocklist: list[str] = field(default_factory=lambda: _csv_env("OF_SYMBOLS_BLOCKLIST"))

    telegram_token: str | None = os.getenv("TELEGRAM_BOT_TOKEN") or None
    telegram_chat_id: str | None = os.getenv("TELEGRAM_CHAT_ID") or None

    @property
    def rest_base_url(self) -> str:
        return "https://api-testnet.bybit.com" if self.bybit_testnet else "https://api.bybit.com"

    @property
    def ws_public_url(self) -> str:
        base = "wss://stream-testnet.bybit.com" if self.bybit_testnet else "wss://stream.bybit.com"
        return f"{base}/v5/public/linear"
