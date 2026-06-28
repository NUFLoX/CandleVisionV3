from __future__ import annotations

import asyncio
import csv
import json
import os
import sqlite3
import sys
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orderflow_accum.bybit_rest import BybitRestClient
from orderflow_accum.config import Settings


SOURCE_NAME = "coinpaprika_tickers_v1"
BYBIT_SPOT_MARKETS_SOURCE = "coinpaprika_bybit_spot_markets_v1"
HOUR_MS = 60 * 60 * 1000

BUCKETS: list[tuple[str, float]] = [
    ("CAP_LT_20M", 20_000_000.0),
    ("CAP_20M_100M", 100_000_000.0),
    ("CAP_100M_300M", 300_000_000.0),
    ("CAP_300M_600M", 600_000_000.0),
    ("CAP_600M_1B", 1_000_000_000.0),
    ("CAP_1B_3B", 3_000_000_000.0),
]

BUCKET_ORDER = {
    "CAP_LT_20M": 0,
    "CAP_20M_100M": 1,
    "CAP_100M_300M": 2,
    "CAP_300M_600M": 3,
    "CAP_600M_1B": 4,
    "CAP_1B_3B": 5,
    "CAP_GT_3B": 6,
    "CAP_AMBIGUOUS": 98,
    "CAP_MISSING": 99,
}

CAP_PRIORITY = {
    "CAP_LT_20M": 6.0,
    "CAP_20M_100M": 5.0,
    "CAP_100M_300M": 4.0,
    "CAP_300M_600M": 3.0,
    "CAP_600M_1B": 2.0,
    "CAP_1B_3B": 1.0,
    "CAP_GT_3B": 0.5,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def clip(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def pct_change(current: float, previous: float) -> float:
    if previous <= 0:
        return 0.0
    return ((current / previous) - 1.0) * 100.0


def cap_bucket(market_cap: float, cap_status: str) -> str:
    if cap_status == "CAP_AMBIGUOUS":
        return "CAP_AMBIGUOUS"

    if cap_status != "CAP_VERIFIED" or market_cap <= 0:
        return "CAP_MISSING"

    for name, upper_bound in BUCKETS:
        if market_cap < upper_bound:
            return name

    return "CAP_GT_3B"


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universal_radar_source_cache (
            source_name TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            refreshed_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universal_radar_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            cap_source TEXT NOT NULL,
            cap_source_cached INTEGER NOT NULL DEFAULT 0,
            bybit_pairs INTEGER NOT NULL DEFAULT 0,
            liquid_pairs INTEGER NOT NULL DEFAULT 0,
            unique_cap_matches INTEGER NOT NULL DEFAULT 0,
            ambiguous_cap_matches INTEGER NOT NULL DEFAULT 0,
            missing_cap_matches INTEGER NOT NULL DEFAULT 0,
            base_candidates INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS universal_radar_candidates (
            run_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            base_asset TEXT NOT NULL,
            market TEXT NOT NULL,
            cap_status TEXT NOT NULL,
            cap_match_count INTEGER NOT NULL DEFAULT 0,
            coinpaprika_id TEXT,
            market_cap_usd REAL,
            market_cap_rank INTEGER,
            cap_bucket TEXT NOT NULL,
            last_price REAL NOT NULL DEFAULT 0,
            price24h_pct REAL NOT NULL DEFAULT 0,
            turnover24h REAL NOT NULL DEFAULT 0,
            turnover4h REAL,
            turnover1h REAL,
            liquidity_to_cap REAL,
            h1_closed_bars INTEGER NOT NULL DEFAULT 0,
            base_range_24h_pct REAL,
            range_6h_pct REAL,
            return_1h_pct REAL,
            return_4h_pct REAL,
            return_6h_pct REAL,
            volume_stability REAL,
            late_impulse INTEGER NOT NULL DEFAULT 0,
            late_reasons TEXT,
            base_candidate INTEGER NOT NULL DEFAULT 0,
            radar_score REAL NOT NULL DEFAULT 0,
            h1_status TEXT NOT NULL DEFAULT '',
            h1_error TEXT,
            PRIMARY KEY (run_id, symbol)
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_universal_radar_candidates_run
        ON universal_radar_candidates(run_id, radar_score)
        """
    )

    conn.commit()


def cache_is_fresh(refreshed_at: str, ttl_seconds: int) -> bool:
    try:
        refreshed = datetime.fromisoformat(refreshed_at.replace("Z", "+00:00"))
    except ValueError:
        return False

    age_seconds = (utc_now() - refreshed.astimezone(timezone.utc)).total_seconds()
    return 0 <= age_seconds < ttl_seconds


async def load_coinpaprika_tickers(
    conn: sqlite3.Connection,
    timeout_seconds: int,
    ttl_seconds: int,
) -> tuple[list[dict[str, Any]], bool]:
    row = conn.execute(
        """
        SELECT payload_json, refreshed_at
        FROM universal_radar_source_cache
        WHERE source_name=?
        """,
        (SOURCE_NAME,),
    ).fetchone()

    if row and cache_is_fresh(str(row["refreshed_at"]), ttl_seconds):
        try:
            payload = json.loads(str(row["payload_json"]))
            if isinstance(payload, list):
                return payload, True
        except json.JSONDecodeError:
            pass

    base_url = os.getenv(
        "COINPAPRIKA_BASE_URL",
        "https://api.coinpaprika.com/v1",
    ).rstrip("/")

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={
            "Accept": "application/json",
            "User-Agent": "CandleVisionUniversalRadar/1.0",
        },
    ) as session:
        async with session.get(f"{base_url}/tickers") as response:
            response.raise_for_status()
            payload = await response.json()

    if not isinstance(payload, list):
        raise RuntimeError("CoinPaprika tickers response is not a list")

    conn.execute(
        """
        INSERT INTO universal_radar_source_cache (
            source_name,
            payload_json,
            refreshed_at
        )
        VALUES (?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            payload_json=excluded.payload_json,
            refreshed_at=excluded.refreshed_at
        """,
        (
            SOURCE_NAME,
            json.dumps(payload, separators=(",", ":")),
            iso_now(),
        ),
    )

    conn.commit()
    return payload, False


async def load_coinpaprika_bybit_spot_markets(
    conn: sqlite3.Connection,
    timeout_seconds: int,
    ttl_seconds: int,
) -> tuple[list[dict[str, Any]], bool]:
    row = conn.execute(
        """
        SELECT payload_json, refreshed_at
        FROM universal_radar_source_cache
        WHERE source_name=?
        """,
        (BYBIT_SPOT_MARKETS_SOURCE,),
    ).fetchone()

    if row and cache_is_fresh(str(row["refreshed_at"]), ttl_seconds):
        try:
            payload = json.loads(str(row["payload_json"]))
            if isinstance(payload, list):
                return payload, True
        except json.JSONDecodeError:
            pass

    base_url = os.getenv(
        "COINPAPRIKA_BASE_URL",
        "https://api.coinpaprika.com/v1",
    ).rstrip("/")

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={
            "Accept": "application/json",
            "User-Agent": "CandleVisionUniversalRadar/1.0",
        },
    ) as session:
        async with session.get(
            f"{base_url}/exchanges/bybit-spot/markets"
        ) as response:
            response.raise_for_status()
            payload = await response.json()

    if not isinstance(payload, list):
        raise RuntimeError(
            "CoinPaprika Bybit spot markets response is not a list"
        )

    conn.execute(
        """
        INSERT INTO universal_radar_source_cache (
            source_name,
            payload_json,
            refreshed_at
        )
        VALUES (?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            payload_json=excluded.payload_json,
            refreshed_at=excluded.refreshed_at
        """,
        (
            BYBIT_SPOT_MARKETS_SOURCE,
            json.dumps(payload, separators=(",", ":")),
            iso_now(),
        ),
    )

    conn.commit()
    return payload, False


def build_cap_index_by_id(
    coins: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}

    for coin in coins:
        coin_id = str(coin.get("id") or "").strip()
        usd = (coin.get("quotes") or {}).get("USD") or {}
        market_cap = as_float(usd.get("market_cap"))

        if not coin_id or market_cap <= 0:
            continue

        index[coin_id] = {
            "id": coin_id,
            "market_cap": market_cap,
            "rank": as_int(coin.get("rank")),
        }

    return index


def build_bybit_spot_base_index(
    markets: list[dict[str, Any]],
) -> dict[str, list[str]]:
    index: dict[str, set[str]] = defaultdict(set)

    for market in markets:
        pair = str(market.get("pair") or "").upper().strip()
        coin_id = str(
            market.get("base_currency_id") or ""
        ).strip()

        if not pair.endswith("/USDT") or not coin_id:
            continue

        base_asset = pair.rsplit("/", 1)[0].strip()

        if base_asset:
            index[base_asset].add(coin_id)

    return {
        base_asset: sorted(coin_ids)
        for base_asset, coin_ids in index.items()
    }


class AsyncRequestPacer:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = max(
            0.0,
            float(min_interval_seconds),
        )
        self._next_allowed_at = 0.0
        self._lock = asyncio.Lock()

    async def wait_turn(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait_seconds = max(
                0.0,
                self._next_allowed_at - now,
            )

            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            self._next_allowed_at = (
                time.monotonic()
                + self.min_interval_seconds
            )

    async def defer(self, delay_seconds: float) -> None:
        async with self._lock:
            self._next_allowed_at = max(
                self._next_allowed_at,
                time.monotonic() + max(0.0, delay_seconds),
            )


def is_bybit_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()

    return (
        "retcode': 10006" in message
        or '"retcode": 10006' in message
        or "exceeded the api rate limit" in message
    )


def calculate_h1_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "h1_closed_bars": 0,
            "h1_status": "request_error",
            "h1_error": "empty_kline_response",
        }

    now_ms = int(time.time() * 1000)
    current_h1_open = (now_ms // HOUR_MS) * HOUR_MS

    frame = frame[
        pd.to_numeric(frame["start"], errors="coerce") < current_h1_open
    ].copy()

    for column in ["high", "low", "close", "turnover"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["high", "low", "close", "turnover"])

    if len(frame) < 8:
        return {
            "h1_closed_bars": int(len(frame)),
            "h1_status": "insufficient_history",
            "h1_error": "not_enough_closed_h1_bars",
        }

    closes = frame["close"].astype(float).tolist()
    turnovers = frame["turnover"].astype(float).tolist()

    def range_pct(hours: int) -> float:
        part = frame.tail(hours)
        close = as_float(part["close"].iloc[-1])
        high = as_float(part["high"].max())
        low = as_float(part["low"].min())

        if close <= 0 or high <= low:
            return 0.0

        return ((high - low) / close) * 100.0

    recent_turnovers = turnovers[-12:]
    mean_turnover = sum(recent_turnovers) / max(len(recent_turnovers), 1)
    median_turnover = statistics.median(recent_turnovers)

    return {
        "h1_closed_bars": int(len(frame)),
        "h1_status": "ok",
        "turnover1h": turnovers[-1],
        "turnover4h": sum(turnovers[-4:]),
        "base_range_24h_pct": range_pct(min(24, len(frame))),
        "range_6h_pct": range_pct(6),
        "return_1h_pct": pct_change(closes[-1], closes[-2]),
        "return_4h_pct": pct_change(closes[-1], closes[-5]),
        "return_6h_pct": pct_change(closes[-1], closes[-7]),
        "volume_stability": clip(
            median_turnover / max(mean_turnover, 1e-12),
            0.0,
            1.0,
        ),
        "h1_error": "",
    }


async def fetch_h1_metrics(
    rest: BybitRestClient,
    semaphore: asyncio.Semaphore,
    pacer: AsyncRequestPacer,
    symbol: str,
    kline_limit: int,
    rate_limit_retries: int,
    rate_limit_backoff_seconds: float,
) -> tuple[str, dict[str, Any]]:
    async with semaphore:
        for attempt in range(rate_limit_retries + 1):
            await pacer.wait_turn()

            try:
                frame = await rest.fetch_klines(
                    symbol=symbol,
                    interval="60",
                    limit=kline_limit,
                    category="linear",
                )

                return symbol, calculate_h1_metrics(frame)

            except Exception as exc:
                is_rate_limited = is_bybit_rate_limit_error(exc)

                if is_rate_limited and attempt < rate_limit_retries:
                    cooldown_seconds = (
                        rate_limit_backoff_seconds
                        * (2 ** attempt)
                    )
                    await pacer.defer(cooldown_seconds)
                    continue

                return symbol, {
                    "h1_closed_bars": 0,
                    "h1_status": "request_error",
                    "h1_error": (
                        f"attempts={attempt + 1}; "
                        f"{type(exc).__name__}: {str(exc)[:160]}"
                    ),
                }

        return symbol, {
            "h1_closed_bars": 0,
            "h1_status": "request_error",
            "h1_error": "unexpected_h1_retry_exhaustion",
        }


def late_impulse_reasons(
    price24h_pct: float,
    return_1h_pct: float,
    return_4h_pct: float,
    return_6h_pct: float,
    max_24h_pct: float,
    max_1h_pct: float,
    max_4h_pct: float,
    max_6h_pct: float,
) -> list[str]:
    reasons: list[str] = []

    if price24h_pct >= max_24h_pct:
        reasons.append("late_24h_move")

    if return_1h_pct >= max_1h_pct:
        reasons.append("late_1h_move")

    if return_4h_pct >= max_4h_pct:
        reasons.append("late_4h_move")

    if return_6h_pct >= max_6h_pct:
        reasons.append("late_6h_move")

    return reasons


def calculate_radar_score(
    cap_status: str,
    bucket: str,
    liquidity_to_cap: float | None,
    base_range_24h_pct: float | None,
    max_base_range_pct: float,
    volume_stability: float | None,
    is_late: bool,
) -> float:
    if cap_status != "CAP_VERIFIED":
        return 0.0

    cap_score = CAP_PRIORITY.get(bucket, 0.0)
    liquidity_score = clip(as_float(liquidity_to_cap), 0.0, 2.5) * 1.6

    compression_score = max(
        0.0,
        1.0 - (
            as_float(base_range_24h_pct)
            / max(max_base_range_pct, 1e-12)
        ),
    ) * 3.0

    stability_score = clip(as_float(volume_stability), 0.0, 1.0) * 2.0
    late_penalty = 5.0 if is_late else 0.0

    return round(
        max(
            cap_score
            + liquidity_score
            + compression_score
            + stability_score
            - late_penalty,
            0.0,
        ),
        3,
    )


def insert_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, Any]],
) -> None:
    conn.executemany(
        """
        INSERT OR REPLACE INTO universal_radar_candidates (
            run_id,
            observed_at,
            symbol,
            base_asset,
            market,
            cap_status,
            cap_match_count,
            coinpaprika_id,
            market_cap_usd,
            market_cap_rank,
            cap_bucket,
            last_price,
            price24h_pct,
            turnover24h,
            turnover4h,
            turnover1h,
            liquidity_to_cap,
            h1_closed_bars,
            base_range_24h_pct,
            range_6h_pct,
            return_1h_pct,
            return_4h_pct,
            return_6h_pct,
            volume_stability,
            late_impulse,
            late_reasons,
            base_candidate,
            radar_score,
            h1_status,
            h1_error
        )
        VALUES (
            :run_id,
            :observed_at,
            :symbol,
            :base_asset,
            :market,
            :cap_status,
            :cap_match_count,
            :coinpaprika_id,
            :market_cap_usd,
            :market_cap_rank,
            :cap_bucket,
            :last_price,
            :price24h_pct,
            :turnover24h,
            :turnover4h,
            :turnover1h,
            :liquidity_to_cap,
            :h1_closed_bars,
            :base_range_24h_pct,
            :range_6h_pct,
            :return_1h_pct,
            :return_4h_pct,
            :return_6h_pct,
            :volume_stability,
            :late_impulse,
            :late_reasons,
            :base_candidate,
            :radar_score,
            :h1_status,
            :h1_error
        )
        """,
        rows,
    )

    conn.commit()


def write_reports(rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    ordered = sorted(
        rows,
        key=lambda row: (
            0 if row["base_candidate"] else 1,
            BUCKET_ORDER.get(str(row["cap_bucket"]), 100),
            -as_float(row["radar_score"]),
            -as_float(row["turnover24h"]),
            str(row["symbol"]),
        ),
    )

    keys = [
        "symbol",
        "base_asset",
        "cap_bucket",
        "cap_status",
        "cap_match_count",
        "coinpaprika_id",
        "market_cap_usd",
        "market_cap_rank",
        "last_price",
        "price24h_pct",
        "turnover24h",
        "turnover4h",
        "turnover1h",
        "liquidity_to_cap",
        "base_range_24h_pct",
        "range_6h_pct",
        "return_1h_pct",
        "return_4h_pct",
        "return_6h_pct",
        "volume_stability",
        "late_impulse",
        "late_reasons",
        "base_candidate",
        "radar_score",
        "h1_closed_bars",
        "h1_status",
        "h1_error",
    ]

    report_rows = [
        {key: row.get(key) for key in keys}
        for row in ordered
    ]

    (reports_dir / "universal_radar_latest.json").write_text(
        json.dumps(report_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    (reports_dir / "universal_radar_summary_latest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    csv_path = reports_dir / "universal_radar_latest.csv"

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(report_rows)


def summarize_buckets(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}

    buckets = sorted(
        {str(row["cap_bucket"]) for row in rows},
        key=lambda bucket: BUCKET_ORDER.get(bucket, 100),
    )

    for bucket in buckets:
        bucket_rows = [
            row for row in rows
            if row["cap_bucket"] == bucket
        ]

        result[bucket] = {
            "pairs": len(bucket_rows),
            "liquid_pairs": sum(
                1 for row in bucket_rows
                if row["is_liquid"]
            ),
            "base_candidates": sum(
                1 for row in bucket_rows
                if row["base_candidate"]
            ),
        }

    return result


async def main() -> None:
    if not env_bool("UNIVERSAL_RADAR_REPORT_ONLY", True):
        raise SystemExit(
            "Refusing to run: UNIVERSAL_RADAR_REPORT_ONLY must be true"
        )

    settings = Settings()

    db_path = os.getenv(
        "UNIVERSAL_RADAR_DB_PATH",
        "data/universal_radar.db",
    )

    min_turnover_24h = env_float(
        "UNIVERSAL_RADAR_MIN_TURNOVER_24H",
        1_000_000.0,
    )

    min_turnover_to_cap = env_float(
        "UNIVERSAL_RADAR_MIN_TURNOVER_TO_CAP",
        0.03,
    )

    max_base_range_pct = env_float(
        "UNIVERSAL_RADAR_MAX_BASE_RANGE_24H_PCT",
        14.0,
    )

    min_volume_stability = env_float(
        "UNIVERSAL_RADAR_MIN_VOLUME_STABILITY",
        0.45,
    )

    max_24h_pct = env_float(
        "UNIVERSAL_RADAR_MAX_24H_MOVE_PCT",
        25.0,
    )

    max_1h_pct = env_float(
        "UNIVERSAL_RADAR_MAX_1H_MOVE_PCT",
        5.0,
    )

    max_4h_pct = env_float(
        "UNIVERSAL_RADAR_MAX_4H_MOVE_PCT",
        10.0,
    )

    max_6h_pct = env_float(
        "UNIVERSAL_RADAR_MAX_6H_MOVE_PCT",
        12.0,
    )

    kline_limit = max(
        48,
        env_int("UNIVERSAL_RADAR_KLINE_LIMIT", 96),
    )

    concurrency = max(
        1,
        min(
            env_int("UNIVERSAL_RADAR_KLINE_CONCURRENCY", 3),
            8,
        ),
    )

    kline_min_interval_seconds = max(
        0.25,
        env_float(
            "UNIVERSAL_RADAR_KLINE_MIN_INTERVAL_SECONDS",
            1.0,
        ),
    )

    kline_rate_limit_retries = max(
        0,
        min(
            env_int(
                "UNIVERSAL_RADAR_KLINE_RATE_LIMIT_RETRIES",
                4,
            ),
            8,
        ),
    )

    kline_rate_limit_backoff_seconds = max(
        1.0,
        env_float(
            "UNIVERSAL_RADAR_KLINE_RATE_LIMIT_BACKOFF_SECONDS",
            5.0,
        ),
    )

    cap_timeout = max(
        10,
        env_int("COINPAPRIKA_TIMEOUT_SECONDS", 45),
    )

    cap_cache_ttl = max(
        900,
        env_int(
            "COINPAPRIKA_MARKET_CAP_CACHE_TTL_SECONDS",
            21600,
        ),
    )

    run_id = utc_now().strftime("%Y%m%dT%H%M%SZ")
    observed_at = iso_now()

    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row

    ensure_schema(conn)

    print("=== UNIVERSAL MARKET RADAR V1 ===")
    print("mode: report-only")
    print("db:", db_file)
    print("quote:", settings.quote_coin)
    print("min_turnover_24h:", f"${min_turnover_24h:,.0f}")
    print("kline_concurrency:", concurrency)
    print(
        "kline_min_interval_seconds:",
        kline_min_interval_seconds,
    )
    print(
        "kline_rate_limit_retries:",
        kline_rate_limit_retries,
    )

    conn.execute(
        """
        INSERT OR REPLACE INTO universal_radar_runs (
            run_id,
            started_at,
            cap_source
        )
        VALUES (?, ?, ?)
        """,
        (
            run_id,
            observed_at,
            "coinpaprika",
        ),
    )

    conn.commit()

    try:
        coins, tickers_cache_hit = await load_coinpaprika_tickers(
            conn=conn,
            timeout_seconds=cap_timeout,
            ttl_seconds=cap_cache_ttl,
        )

        spot_markets, spot_markets_cache_hit = (
            await load_coinpaprika_bybit_spot_markets(
                conn=conn,
                timeout_seconds=cap_timeout,
                ttl_seconds=cap_cache_ttl,
            )
        )

        cap_index = build_cap_index_by_id(coins)
        bybit_spot_index = build_bybit_spot_base_index(
            spot_markets
        )

        async with BybitRestClient(
            settings.rest_base_url,
            timeout_seconds=settings.rest_timeout_seconds,
            retries=settings.rest_retries,
        ) as bybit:
            instruments = await bybit.fetch_linear_symbols(
                settings.quote_coin
            )

            tickers = await bybit.fetch_tickers(category="linear")

            instrument_map = {
                str(item.get("symbol") or "").upper():
                str(item.get("baseCoin") or "").upper()
                for item in instruments
                if item.get("symbol") and item.get("baseCoin")
            }

            rows: list[dict[str, Any]] = []

            for ticker in tickers:
                symbol = str(ticker.get("symbol") or "").upper()
                base_asset = instrument_map.get(symbol, "")

                if not symbol or not base_asset:
                    continue

                last_price = as_float(ticker.get("lastPrice"))
                turnover24h = as_float(ticker.get("turnover24h"))

                if last_price <= 0 or turnover24h <= 0:
                    continue

                spot_coin_ids = bybit_spot_index.get(
                    base_asset,
                    [],
                )

                if len(spot_coin_ids) == 1:
                    coinpaprika_id = spot_coin_ids[0]
                    cap = cap_index.get(coinpaprika_id)

                    if cap:
                        cap_status = "CAP_VERIFIED"
                        market_cap = as_float(cap["market_cap"])
                        market_cap_rank = as_int(cap["rank"])
                    else:
                        cap_status = "CAP_MISSING"
                        market_cap = 0.0
                        market_cap_rank = 0

                elif len(spot_coin_ids) > 1:
                    cap_status = "CAP_AMBIGUOUS"
                    coinpaprika_id = ""
                    market_cap = 0.0
                    market_cap_rank = 0

                else:
                    cap_status = "CAP_MISSING"
                    coinpaprika_id = ""
                    market_cap = 0.0
                    market_cap_rank = 0

                rows.append(
                    {
                        "run_id": run_id,
                        "observed_at": observed_at,
                        "symbol": symbol,
                        "base_asset": base_asset,
                        "market": "linear",
                        "cap_status": cap_status,
                        "cap_match_count": len(spot_coin_ids),
                        "coinpaprika_id": coinpaprika_id,
                        "market_cap_usd": (
                            market_cap
                            if market_cap > 0
                            else None
                        ),
                        "market_cap_rank": (
                            market_cap_rank
                            if market_cap_rank > 0
                            else None
                        ),
                        "cap_bucket": cap_bucket(
                            market_cap,
                            cap_status,
                        ),
                        "last_price": last_price,
                        "price24h_pct": (
                            as_float(ticker.get("price24hPcnt")) * 100.0
                        ),
                        "turnover24h": turnover24h,
                        "is_liquid": (
                            turnover24h >= min_turnover_24h
                        ),
                    }
                )

            liquid_symbols = [
                row["symbol"]
                for row in rows
                if row["is_liquid"]
            ]

            print("bybit_pairs:", len(rows))
            print("liquid_pairs_for_h1:", len(liquid_symbols))
            print(
                "coinpaprika_tickers_cache:",
                "hit" if tickers_cache_hit else "refreshed",
            )
            print(
                "coinpaprika_bybit_spot_cache:",
                "hit" if spot_markets_cache_hit else "refreshed",
            )

            semaphore = asyncio.Semaphore(concurrency)
            request_pacer = AsyncRequestPacer(
                kline_min_interval_seconds
            )

            metric_pairs = await asyncio.gather(
                *[
                    fetch_h1_metrics(
                        rest=bybit,
                        semaphore=semaphore,
                        pacer=request_pacer,
                        symbol=symbol,
                        kline_limit=kline_limit,
                        rate_limit_retries=kline_rate_limit_retries,
                        rate_limit_backoff_seconds=(
                            kline_rate_limit_backoff_seconds
                        ),
                    )
                    for symbol in liquid_symbols
                ]
            )

        metrics_by_symbol = dict(metric_pairs)

        for row in rows:
            metric_defaults = {
                "h1_closed_bars": 0,
                "turnover1h": None,
                "turnover4h": None,
                "base_range_24h_pct": None,
                "range_6h_pct": None,
                "return_1h_pct": None,
                "return_4h_pct": None,
                "return_6h_pct": None,
                "volume_stability": None,
                "h1_status": "not_requested_below_liquidity_floor",
                "h1_error": "",
            }

            metrics = dict(metric_defaults)
            metrics.update(metrics_by_symbol.get(row["symbol"], {}))

            market_cap = as_float(row["market_cap_usd"])

            liquidity_to_cap = (
                row["turnover24h"] / market_cap
                if row["cap_status"] == "CAP_VERIFIED"
                and market_cap > 0
                else None
            )

            late_reasons = late_impulse_reasons(
                price24h_pct=row["price24h_pct"],
                return_1h_pct=as_float(
                    metrics.get("return_1h_pct")
                ),
                return_4h_pct=as_float(
                    metrics.get("return_4h_pct")
                ),
                return_6h_pct=as_float(
                    metrics.get("return_6h_pct")
                ),
                max_24h_pct=max_24h_pct,
                max_1h_pct=max_1h_pct,
                max_4h_pct=max_4h_pct,
                max_6h_pct=max_6h_pct,
            )

            is_late = bool(late_reasons)

            enough_h1 = (
                as_int(metrics.get("h1_closed_bars")) >= 24
            )

            base_candidate = bool(
                row["is_liquid"]
                and row["cap_status"] == "CAP_VERIFIED"
                and enough_h1
                and not is_late
                and as_float(liquidity_to_cap) >= min_turnover_to_cap
                and as_float(
                    metrics.get("base_range_24h_pct")
                ) <= max_base_range_pct
                and as_float(
                    metrics.get("volume_stability")
                ) >= min_volume_stability
            )

            row.update(metrics)

            row.update(
                {
                    "turnover4h": metrics.get("turnover4h"),
                    "turnover1h": metrics.get("turnover1h"),
                    "liquidity_to_cap": liquidity_to_cap,
                    "late_impulse": int(is_late),
                    "late_reasons": ",".join(late_reasons),
                    "base_candidate": int(base_candidate),
                    "radar_score": calculate_radar_score(
                        cap_status=row["cap_status"],
                        bucket=row["cap_bucket"],
                        liquidity_to_cap=liquidity_to_cap,
                        base_range_24h_pct=metrics.get(
                            "base_range_24h_pct"
                        ),
                        max_base_range_pct=max_base_range_pct,
                        volume_stability=metrics.get(
                            "volume_stability"
                        ),
                        is_late=is_late,
                    ),
                }
            )

        insert_rows(conn, rows)

        summary = {
            "run_id": run_id,
            "observed_at": observed_at,
            "mode": "report-only",
            "cap_source": "coinpaprika",
            "cap_source_cached": bool(tickers_cache_hit and spot_markets_cache_hit),
            "pair_count": len(rows),
            "liquid_pair_count": sum(
                1 for row in rows
                if row["is_liquid"]
            ),
            "unique_cap_matches": sum(
                1 for row in rows
                if row["cap_status"] == "CAP_VERIFIED"
            ),
            "ambiguous_cap_matches": sum(
                1 for row in rows
                if row["cap_status"] == "CAP_AMBIGUOUS"
            ),
            "missing_cap_matches": sum(
                1 for row in rows
                if row["cap_status"] == "CAP_MISSING"
            ),
            "base_candidate_count": sum(
                1 for row in rows
                if row["base_candidate"]
            ),
            "h1_completed_count": sum(
                1 for row in rows
                if row.get("h1_status") == "ok"
            ),
            "h1_not_requested_count": sum(
                1 for row in rows
                if row.get("h1_status") == "not_requested_below_liquidity_floor"
            ),
            "h1_insufficient_history_count": sum(
                1 for row in rows
                if row.get("h1_status") == "insufficient_history"
            ),
            "h1_request_error_count": sum(
                1 for row in rows
                if row.get("h1_status") == "request_error"
            ),
            "buckets": summarize_buckets(rows),
        }

        write_reports(rows, summary)

        conn.execute(
            """
            UPDATE universal_radar_runs
            SET
                finished_at=?,
                cap_source_cached=?,
                bybit_pairs=?,
                liquid_pairs=?,
                unique_cap_matches=?,
                ambiguous_cap_matches=?,
                missing_cap_matches=?,
                base_candidates=?,
                error_count=?
            WHERE run_id=?
            """,
            (
                iso_now(),
                int(tickers_cache_hit and spot_markets_cache_hit),
                summary["pair_count"],
                summary["liquid_pair_count"],
                summary["unique_cap_matches"],
                summary["ambiguous_cap_matches"],
                summary["missing_cap_matches"],
                summary["base_candidate_count"],
                summary["h1_request_error_count"],
                run_id,
            ),
        )

        conn.commit()

        print()
        print("=== RESULT ===")
        print("pairs:", summary["pair_count"])
        print("liquid_pairs:", summary["liquid_pair_count"])
        print("verified_bybit_spot_cap_matches:", summary["unique_cap_matches"])
        print("ambiguous_cap_matches:", summary["ambiguous_cap_matches"])
        print("missing_cap_matches:", summary["missing_cap_matches"])
        print("base_candidates:", summary["base_candidate_count"])
        print("h1_completed:", summary["h1_completed_count"])
        print("h1_not_requested:", summary["h1_not_requested_count"])
        print("h1_insufficient_history:", summary["h1_insufficient_history_count"])
        print("h1_request_errors:", summary["h1_request_error_count"])
        print("reports/universal_radar_latest.csv")
        print("reports/universal_radar_latest.json")
        print("reports/universal_radar_summary_latest.json")

        candidates = [
            row for row in rows
            if row["base_candidate"]
        ]

        candidates.sort(
            key=lambda row: (
                BUCKET_ORDER.get(
                    str(row["cap_bucket"]),
                    100,
                ),
                -as_float(row["radar_score"]),
                -as_float(row["turnover24h"]),
            )
        )

        print()
        print("=== TOP 20 BASE CANDIDATES ===")

        for row in candidates[:20]:
            print(
                f'{row["symbol"]:16} '
                f'{row["cap_bucket"]:16} '
                f'cap=${as_float(row["market_cap_usd"]) / 1_000_000:8.2f}M '
                f'24h=${as_float(row["turnover24h"]) / 1_000_000:7.2f}M '
                f'liq/cap={as_float(row["liquidity_to_cap"]):5.2f} '
                f'base24h={as_float(row.get("base_range_24h_pct")):5.2f}% '
                f'ret6h={as_float(row.get("return_6h_pct")):5.2f}% '
                f'score={as_float(row["radar_score"]):5.2f}'
            )

    except Exception:
        conn.execute(
            """
            UPDATE universal_radar_runs
            SET finished_at=?, error_count=1
            WHERE run_id=?
            """,
            (
                iso_now(),
                run_id,
            ),
        )

        conn.commit()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
