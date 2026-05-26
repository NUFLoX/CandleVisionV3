# -*- coding: utf-8 -*-
import os
from pathlib import Path

# Легкий .env loader без обязательной runtime-зависимости от python-dotenv.
def _load_dotenv(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


# Загружаем ключи из скрытого файла .env
_load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


# --- Секреты и окружение ---
# Реальные значения должны жить только в .env/секрет-хранилище, не в git.
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()

# BYBIT_TESTNET — новый единый флаг; TESTNET оставлен для обратной совместимости.
BYBIT_TESTNET = _env_bool("BYBIT_TESTNET", _env_bool("TESTNET", False))
SIGNALS_ONLY = _env_bool("SIGNALS_ONLY", True)
BYBIT_REST_BASE_URL = "https://api-testnet.bybit.com" if BYBIT_TESTNET else "https://api.bybit.com"
BYBIT_WS_PUBLIC_URL = (
    "wss://stream-testnet.bybit.com/v5/public/linear"
    if BYBIT_TESTNET
    else "wss://stream.bybit.com/v5/public/linear"
)

# ==========================================
# ⚙️ ГЛОБАЛЬНЫЕ НАСТРОЙКИ CANDLEVISION
# ==========================================

# --- Настройки API и сети ---
API_RETRY_COUNT = 3
API_TIMEOUT = 10
SCAN_DELAY = 0.2

# --- Тайминги Оркестратора ---
SCAN_INTERVAL = 60
WATCHLIST_INTERVAL = 180
DANGER_PAUSE = 180

# --- Настройки Торговли ---
RISK_PERCENT = 1.0
INITIAL_BALANCE = 1000
SCORE_TO_WATCH = 1.5
SCORE_TO_TRADE = 2.5


def _env_csv(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip() for item in raw.split(',') if item.strip()]


# Таймфреймы сканирования для текущего Scout.
# По умолчанию: 1m + добавленные 5m/15m/30m.
SCOUT_SCAN_TIMEFRAMES = _env_csv("SCOUT_SCAN_TIMEFRAMES", "1m,5m,15m,30m")


def trading_enabled() -> bool:
    return (not SIGNALS_ONLY) and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)



def trading_enabled() -> bool:
    return (not SIGNALS_ONLY) and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)


def trading_enabled() -> bool:
    return (not SIGNALS_ONLY) and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)

def trading_enabled() -> bool:
    return (not SIGNALS_ONLY) and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)

def trading_enabled() -> bool:
    return (not SIGNALS_ONLY) and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)





