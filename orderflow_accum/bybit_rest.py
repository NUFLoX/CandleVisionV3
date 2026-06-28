from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
import pandas as pd

logger = logging.getLogger("OrderFlow.BybitREST")

@dataclass(slots=True)
class ScanTarget:
    symbol: str
    market: str


class BybitRateLimitError(RuntimeError):
    """Raised when Bybit reports API rate limiting."""


class AsyncRequestPacer:
    """Serialize REST starts for one client and support global cooldowns."""

    def __init__(self, min_interval_seconds: float = 0.0) -> None:
        self.min_interval_seconds = max(
            float(min_interval_seconds),
            0.0,
        )
        self._next_allowed_at = 0.0
        self._lock = asyncio.Lock()

    async def wait_turn(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                wait_seconds = max(
                    0.0,
                    self._next_allowed_at - now,
                )

                if wait_seconds <= 0:
                    self._next_allowed_at = (
                        now + self.min_interval_seconds
                    )
                    return

            await asyncio.sleep(wait_seconds)

    async def defer(self, delay_seconds: float) -> None:
        async with self._lock:
            self._next_allowed_at = max(
                self._next_allowed_at,
                time.monotonic()
                + max(float(delay_seconds), 0.0),
            )


class BybitRestClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 25,
        retries: int = 2,
        min_interval_seconds: float = 0.0,
        rate_limit_retries: int = 0,
        rate_limit_backoff_seconds: float = 1.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.retries = max(int(retries), 0)
        self.rate_limit_retries = max(
            int(rate_limit_retries),
            0,
        )
        self.rate_limit_backoff_seconds = max(
            float(rate_limit_backoff_seconds),
            0.0,
        )
        self._request_pacer = AsyncRequestPacer(
            min_interval_seconds=min_interval_seconds
        )
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "BybitRestClient":
        self._session = aiohttp.ClientSession(timeout=self.timeout, headers={"User-Agent": "CandleVision-OrderFlow/1.0"})
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()

    async def _get(
        self,
        path: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError(
                "BybitRestClient session is not started"
            )

        url = f"{self.base_url}{path}"
        network_attempt = 0
        rate_limit_attempt = 0

        while True:
            await self._request_pacer.wait_turn()

            try:
                async with self._session.get(
                    url,
                    params=params,
                ) as response:
                    response.raise_for_status()
                    data = await response.json()

                ret_code = data.get("retCode")

                if str(ret_code) == "10006":
                    raise BybitRateLimitError(
                        f"Bybit API rate limit: {data}"
                    )

                if ret_code != 0:
                    raise RuntimeError(
                        f"Bybit API error: {data}"
                    )

                return data.get("result", {})

            except BybitRateLimitError:
                if rate_limit_attempt >= self.rate_limit_retries:
                    raise

                cooldown_seconds = (
                    self.rate_limit_backoff_seconds
                    * (2 ** rate_limit_attempt)
                )
                rate_limit_attempt += 1

                logger.warning(
                    "Bybit rate limit on %s; retry %s/%s after %.2fs",
                    path,
                    rate_limit_attempt,
                    self.rate_limit_retries,
                    cooldown_seconds,
                )

                await self._request_pacer.defer(
                    cooldown_seconds
                )
                continue

            except aiohttp.ClientResponseError as exc:
                if (
                    exc.status == 429
                    and rate_limit_attempt
                    < self.rate_limit_retries
                ):
                    cooldown_seconds = (
                        self.rate_limit_backoff_seconds
                        * (2 ** rate_limit_attempt)
                    )
                    rate_limit_attempt += 1

                    logger.warning(
                        "Bybit HTTP 429 on %s; retry %s/%s after %.2fs",
                        path,
                        rate_limit_attempt,
                        self.rate_limit_retries,
                        cooldown_seconds,
                    )

                    await self._request_pacer.defer(
                        cooldown_seconds
                    )
                    continue

                if network_attempt >= self.retries:
                    raise

                network_attempt += 1
                await asyncio.sleep(
                    0.6 * network_attempt
                )

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ):
                if network_attempt >= self.retries:
                    raise

                network_attempt += 1
                await asyncio.sleep(
                    0.6 * network_attempt
                )

    async def fetch_linear_symbols(self, quote_coin: str = "USDT") -> list[dict[str, Any]]:
        result = await self._get(
            "/v5/market/instruments-info",
            {"category": "linear", "limit": 1000, "status": "Trading"},
        )
        rows = result.get("list", [])
        return [row for row in rows if row.get("quoteCoin") == quote_coin and row.get("status") == "Trading"]

    async def fetch_tickers(self, category: str = "linear") -> list[dict[str, Any]]:
        result = await self._get("/v5/market/tickers", {"category": category})
        return result.get("list", [])

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 200,
        start: int | None = None,
        end: int | None = None,
        category: str = "linear",
    ) -> pd.DataFrame:
        params: dict[str, Any] = {"category": category, "symbol": symbol, "interval": interval, "limit": limit}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end
        result = await self._get(
            "/v5/market/kline",
            params,
        )
        rows = result.get("list", [])
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["start", "open", "high", "low", "close", "volume", "turnover"])
        df = df.iloc[::-1].reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = df[col].astype(float)
        df["start"] = pd.to_numeric(df["start"], errors="coerce").astype("Int64")
        return df

    async def fetch_best_symbols(
        self,
        quote_coin: str,
        limit: int,
        min_notional_24h: float,
        min_last_price: float,
        market_categories: list[str] | None = None,
        allowlist: list[str] | None = None,
        blocklist: list[str] | None = None,
    ) -> list[ScanTarget]:
        allow = set(allowlist or [])
        block = set(blocklist or [])
        categories = [c.lower() for c in (market_categories or ["linear"]) if c]
        tickers: list[dict[str, Any]] = []
        for category in categories:
            rows = await self.fetch_tickers(category=category)
            for row in rows:
                row = dict(row)
                row["_category"] = category
                tickers.append(row)

        symbols: list[tuple[str, float, str]] = []

        for row in tickers:
            symbol = row.get("symbol", "")
            if not symbol.endswith(quote_coin):
                continue
            if allow and symbol not in allow:
                continue
            if symbol in block:
                continue
            try:
                turnover = float(row.get("turnover24h") or 0.0)
                last_price = float(row.get("lastPrice") or 0.0)
            except (TypeError, ValueError):
                continue
            if turnover < min_notional_24h or last_price < min_last_price:
                continue
            symbols.append((symbol, turnover, str(row.get("_category", "linear")).lower()))
        symbols.sort(key=lambda item: item[1], reverse=True)
        selected = [ScanTarget(symbol=symbol, market=market) for symbol, _, market in symbols[:limit]]
        logger.info("Selected %s symbols for scan", len(selected))
        return selected
