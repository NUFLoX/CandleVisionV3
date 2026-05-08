from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config.settings import API_TIMEOUT, BYBIT_REST_BASE_URL
from .schemas import BotLog, BotStatus, CoinMetrics, MarketState, PressureStrip
from .store import normalize_symbol


@dataclass(slots=True)
class LiveDashboardData:
    status: BotStatus
    market_state: MarketState
    pressure_strips: list[PressureStrip]
    logs: list[BotLog]
    coin_metrics: dict[str, CoinMetrics]


class LiveDataError(RuntimeError):
    pass


class BybitPublicMarketData:
    def __init__(self, base_url: str = BYBIT_REST_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")

    async def ticker(self, symbol: str) -> dict[str, Any]:
        data = await self._get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        tickers = data.get("result", {}).get("list", [])
        if not tickers:
            raise LiveDataError(f"Bybit returned no ticker for {symbol}")
        return tickers[0]

    async def kline(self, symbol: str, interval: str = "60", limit: int = 200) -> list[dict[str, float]]:
        data = await self._get(
            "/v5/market/kline",
            {"category": "linear", "symbol": symbol, "interval": interval, "limit": str(limit)},
        )
        rows = data.get("result", {}).get("list", [])
        if not rows:
            raise LiveDataError(f"Bybit returned no klines for {symbol}")
        candles = []
        for row in reversed(rows):
            candles.append(
                {
                    "time": _float(row[0]),
                    "open": _float(row[1]),
                    "high": _float(row[2]),
                    "low": _float(row[3]),
                    "close": _float(row[4]),
                    "volume": _float(row[5]),
                    "turnover": _float(row[6]),
                }
            )
        return candles

    async def orderbook(self, symbol: str, limit: int = 50) -> dict[str, Any]:
        return await self._get(
            "/v5/market/orderbook",
            {"category": "linear", "symbol": symbol, "limit": str(limit)},
        )

    async def _get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        data = await asyncio.to_thread(_http_get_json, f"{self.base_url}{path}", params)
        if data.get("retCode") != 0:
            raise LiveDataError(f"Bybit error {data.get('retCode')}: {data.get('retMsg')}")
        return data


async def fetch_live_dashboard_data(symbols: list[str] | None = None) -> LiveDashboardData:
    symbols = [normalize_symbol(symbol) for symbol in (symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"])]
    bybit = BybitPublicMarketData()
    logs: list[BotLog] = []
    status = BotStatus(
        scanner="standby",
        executor="standby",
        telegram="configured-by-env",
        x_delivery="standby",
        bybit_api="CHECKING",
        binance_api="not-configured",
        database="in-memory",
        redis="not-configured",
        rate_limit="checking",
        last_scan_seconds=0,
        open_trades=0,
        closed_trades_today=0,
    )

    try:
        coin_metrics = await _fetch_coin_metrics(bybit, symbols)
        status.bybit_api = "OK"
        status.rate_limit = "OK"
        status.scanner = "online"
        logs.append(_log("Bybit public market data loaded: real tickers, candles and orderbook", "gateway", "success"))
    except Exception as exc:
        status.bybit_api = "ERROR"
        status.rate_limit = "unknown"
        coin_metrics = {}
        logs.append(_log(f"Bybit public market data unavailable: {exc}", "gateway", "error"))

    try:
        pressure_strips = await _fetch_global_pressure()
        logs.append(_log("CoinGecko global market data loaded for dominance strips", "market", "success"))
    except Exception as exc:
        pressure_strips = _pressure_from_bybit(coin_metrics)
        logs.append(_log(f"CoinGecko dominance unavailable, using Bybit-derived pressure: {exc}", "market", "warning"))

    market_state = _build_market_state(coin_metrics, pressure_strips)
    return LiveDashboardData(
        status=status,
        market_state=market_state,
        pressure_strips=pressure_strips,
        logs=logs,
        coin_metrics=coin_metrics,
    )


async def fetch_live_coin_metrics(symbol: str) -> CoinMetrics:
    normalized = normalize_symbol(symbol)
    bybit = BybitPublicMarketData()
    return await _fetch_one_coin_metric(bybit, normalized)


async def _fetch_coin_metrics(bybit: BybitPublicMarketData, symbols: list[str]) -> dict[str, CoinMetrics]:
    results = await asyncio.gather(*[_fetch_one_coin_metric(bybit, symbol) for symbol in symbols])
    return {item.symbol: item for item in results}


async def _fetch_one_coin_metric(bybit: BybitPublicMarketData, symbol: str) -> CoinMetrics:
    ticker, klines, orderbook = await asyncio.gather(
        bybit.ticker(symbol),
        bybit.kline(symbol),
        bybit.orderbook(symbol),
    )
    return _coin_metrics_from_market_data(symbol, ticker, klines, orderbook)


async def _fetch_global_pressure() -> list[PressureStrip]:
    data = await asyncio.to_thread(_http_get_json, "https://api.coingecko.com/api/v3/global", {})
    global_data = data.get("data", {})
    market_cap_pct = global_data.get("market_cap_percentage", {})
    btc_d = _float(market_cap_pct.get("btc"))
    eth_d = _float(market_cap_pct.get("eth"))
    usdt_d = _float(market_cap_pct.get("usdt"))
    market_change = _float(global_data.get("market_cap_change_percentage_24h_usd"))
    total3 = max(0.0, min(100.0, 100.0 - btc_d - eth_d))
    return [
        PressureStrip(
            key="btc_cap",
            label="Crypto Market Cap 24h",
            value=_clamp_percent(50 + market_change * 3),
            change_pct=round(market_change, 2),
            direction=_direction(market_change),
            interpretation="Real CoinGecko global crypto market-cap change over 24h.",
        ),
        PressureStrip(
            key="btc_d",
            label="BTC Dominance",
            value=_clamp_percent(btc_d),
            change_pct=0.0,
            direction="flat",
            interpretation="Real BTC market-cap share from CoinGecko global data.",
        ),
        PressureStrip(
            key="usdt_d",
            label="USDT Dominance",
            value=_clamp_percent(usdt_d),
            change_pct=0.0,
            direction="flat",
            interpretation="Real USDT market-cap share from CoinGecko global data.",
        ),
        PressureStrip(
            key="total3",
            label="TOTAL3 Proxy",
            value=_clamp_percent(total3),
            change_pct=round(market_change, 2),
            direction=_direction(market_change),
            interpretation="Real market-cap proxy: 100% minus BTC and ETH dominance.",
        ),
    ]


def _coin_metrics_from_market_data(
    symbol: str,
    ticker: dict[str, Any],
    klines: list[dict[str, float]],
    orderbook: dict[str, Any],
) -> CoinMetrics:
    closes = [row["close"] for row in klines]
    highs = [row["high"] for row in klines]
    lows = [row["low"] for row in klines]
    volumes = [row["volume"] for row in klines]
    signed_turnovers = [row["turnover"] if row["close"] >= row["open"] else -row["turnover"] for row in klines]

    rsi = _rsi(closes)
    atr = _atr(highs, lows, closes)
    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    ema200 = _ema(closes, 200)
    current_price = _float(ticker.get("lastPrice"), closes[-1])
    bid_notional, ask_notional = _book_notional(orderbook)
    imbalance = 0.0 if bid_notional + ask_notional == 0 else (bid_notional - ask_notional) / (bid_notional + ask_notional)
    volume_q95 = _quantile(volumes[-96:], 0.95)
    volume_spike = "q95" if volumes[-1] >= volume_q95 else "normal"
    score = _accumulation_score(rsi, current_price, ema20, ema50, imbalance, volume_spike)
    price_change_24h = _float(ticker.get("price24hPcnt")) * 100

    return CoinMetrics(
        symbol=symbol,
        market_cap_usd=0.0,
        volume_24h_usd=_float(ticker.get("turnover24h"), sum(row["turnover"] for row in klines[-24:])),
        money_inflow_1h_usd=round(sum(signed_turnovers[-1:]), 2),
        money_inflow_4h_usd=round(sum(signed_turnovers[-4:]), 2),
        money_inflow_24h_usd=round(sum(signed_turnovers[-24:]), 2),
        cex_netflow_usd=0.0,
        whale_activity="unavailable-public-api",
        accumulation_score=score,
        orderbook_imbalance=round(float(imbalance), 4),
        rsi=round(float(rsi), 2),
        atr_pct=round(float(atr / current_price * 100), 2) if current_price else 0.0,
        ema20=round(float(ema20), 6),
        ema50=round(float(ema50), 6),
        ema200=round(float(ema200), 6),
        volume_spike=volume_spike,
        support=round(min(lows[-50:]), 6),
        resistance=round(max(highs[-50:]), 6),
        bot_verdict=(
            f"Real Bybit data. Last price {current_price:g}, 24h change {price_change_24h:.2f}%. "
            "Market cap, CEX netflow and whale labels are not exposed by Bybit public derivatives API."
        ),
        updated_at=datetime.now(timezone.utc),
    )


def _http_get_json(url: str, params: dict[str, str]) -> dict[str, Any]:
    full_url = f"{url}?{urlencode(params)}" if params else url
    request = Request(full_url, headers={"User-Agent": "CandleVisionV3/1.0"})
    with urlopen(request, timeout=API_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


def _book_notional(orderbook: dict[str, Any]) -> tuple[float, float]:
    result = orderbook.get("result", {})
    bids = result.get("b", [])
    asks = result.get("a", [])
    bid_notional = sum(_float(price) * _float(size) for price, size in bids)
    ask_notional = sum(_float(price) * _float(size) for price, size in asks)
    return bid_notional, ask_notional


def _pressure_from_bybit(coin_metrics: dict[str, CoinMetrics]) -> list[PressureStrip]:
    scores = [metric.accumulation_score for metric in coin_metrics.values()]
    avg_score = mean(scores) if scores else 0.0
    btc_score = coin_metrics.get("BTCUSDT").accumulation_score if "BTCUSDT" in coin_metrics else avg_score
    usdt_pressure = _clamp_percent(100 - avg_score * 10)
    return [
        PressureStrip(key="btc_cap", label="Bybit BTC Score", value=_clamp_percent(btc_score * 10), change_pct=0, direction="flat", interpretation="Fallback based on real BTCUSDT Bybit indicators."),
        PressureStrip(key="btc_d", label="Bybit BTC Pressure", value=_clamp_percent(btc_score * 10), change_pct=0, direction="flat", interpretation="Fallback based on real BTCUSDT momentum/orderbook."),
        PressureStrip(key="usdt_d", label="Risk-Off Proxy", value=usdt_pressure, change_pct=0, direction="flat", interpretation="Fallback inverse of real Bybit watch symbols' average score."),
        PressureStrip(key="total3", label="Alt Pressure Proxy", value=_clamp_percent(avg_score * 10), change_pct=0, direction="flat", interpretation="Fallback based on real Bybit watch symbols' average score."),
    ]


def _build_market_state(coin_metrics: dict[str, CoinMetrics], pressure_strips: list[PressureStrip]) -> MarketState:
    btc = coin_metrics.get("BTCUSDT")
    btc_score = btc.accumulation_score if btc else 0.0
    usdt = next((strip.value for strip in pressure_strips if strip.key == "usdt_d"), 0.0)
    total3 = next((strip.value for strip in pressure_strips if strip.key == "total3"), 0.0)
    btc_filter = "DANGER" if btc_score < 3.5 else "CAUTION" if btc_score < 5.5 else "STABLE"
    altcoin_mode = "RISK-OFF" if usdt > 8 and total3 < 35 else "SELECTIVE" if total3 < 45 else "RISK-ON"
    liquidity = "HIGH" if btc and btc.volume_24h_usd >= 5_000_000_000 else "MEDIUM"
    market_regime = "BULL" if total3 >= 45 and btc_score >= 5.5 else "BEAR" if btc_score < 3.5 else "RANGE"
    return MarketState(
        btc_filter=btc_filter,
        altcoin_mode=altcoin_mode,
        liquidity=liquidity,
        market_regime=market_regime,
        usdt_dominance_trend="real-time-level",
        total3_strength="strong" if total3 >= 45 else "weak",
        can_emit_alt_signals=btc_filter != "DANGER" and altcoin_mode != "RISK-OFF",
        updated_at=datetime.now(timezone.utc),
    )


def _accumulation_score(rsi: float, price: float, ema20: float, ema50: float, imbalance: float, volume_spike: str) -> float:
    score = 5.0
    if price > ema20:
        score += 1.0
    if ema20 > ema50:
        score += 1.0
    if 45 <= rsi <= 70:
        score += 1.0
    elif rsi > 75:
        score -= 1.0
    score += max(-1.5, min(1.5, imbalance * 3))
    if volume_spike == "q95":
        score += 1.0
    return round(max(0.0, min(10.0, score)), 2)


def _rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    deltas = [values[index] - values[index - 1] for index in range(1, len(values))]
    seed = deltas[-period:]
    gains = [max(delta, 0.0) for delta in seed]
    losses = [abs(min(delta, 0.0)) for delta in seed]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    if len(closes) <= 1:
        return 0.0
    true_ranges = []
    for index in range(1, len(closes)):
        true_ranges.append(max(highs[index] - lows[index], abs(highs[index] - closes[index - 1]), abs(lows[index] - closes[index - 1])))
    sample = true_ranges[-period:]
    return sum(sample) / len(sample) if sample else 0.0


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    multiplier = 2 / (period + 1)
    ema = values[0]
    for value in values[1:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * q)))
    return ordered[index]


def _log(message: str, source: str, severity: str) -> BotLog:
    return BotLog(message=message, source=source, severity=severity)


def _direction(value: float) -> str:
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "flat"


def _clamp_percent(value: float) -> float:
    return round(max(0.0, min(100.0, float(value))), 2)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
