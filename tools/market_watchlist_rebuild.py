from __future__ import annotations

import asyncio
import csv
import fnmatch
import json
import math
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orderflow_accum.bybit_rest import BybitRestClient
from orderflow_accum.config import Settings


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(float(str(raw).strip()))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        return default


def env_csv(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    return [item.strip().upper() for item in str(raw).split(",") if item.strip()]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class MarketTicker:
    symbol: str
    market: str
    last_price: float
    turnover24h: float
    price24h_pct: float


@dataclass(slots=True)
class WatchCandidate:
    symbol: str
    market: str
    phase: str
    score: float
    trade_eligible: bool
    reason: str
    metrics: dict[str, Any]


def matches_any(symbol: str, patterns: list[str]) -> bool:
    s = symbol.upper()
    return any(fnmatch.fnmatch(s, p.upper()) for p in patterns)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL,
            phase TEXT NOT NULL,
            score REAL NOT NULL,
            trade_eligible INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            reason TEXT,
            metrics_json TEXT,
            discovered_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(symbol, market)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_watchlist_active ON market_watchlist(active, score)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_market_watchlist_symbol ON market_watchlist(symbol)")
    conn.commit()


def upsert_candidates(db_path: str, candidates: list[WatchCandidate], ttl_hours: float) -> None:
    now = utc_now()
    now_s = iso(now)
    expires_s = iso(now + timedelta(hours=max(ttl_hours, 1.0)))

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    active_keys = {(c.symbol, c.market) for c in candidates}

    for c in candidates:
        metrics_json = json.dumps(c.metrics, ensure_ascii=False, sort_keys=True)

        conn.execute(
            """
            INSERT INTO market_watchlist (
                symbol, market, phase, score, trade_eligible, active,
                reason, metrics_json, discovered_at, last_seen_at, expires_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, market) DO UPDATE SET
                phase=excluded.phase,
                score=excluded.score,
                trade_eligible=excluded.trade_eligible,
                active=1,
                reason=excluded.reason,
                metrics_json=excluded.metrics_json,
                last_seen_at=excluded.last_seen_at,
                expires_at=excluded.expires_at,
                updated_at=excluded.updated_at
            """,
            (
                c.symbol,
                c.market,
                c.phase,
                float(c.score),
                1 if c.trade_eligible else 0,
                c.reason,
                metrics_json,
                now_s,
                now_s,
                expires_s,
                now_s,
            ),
        )

    if active_keys:
        placeholders = ",".join(["?"] * len(active_keys))
        flat = [f"{symbol}|{market}" for symbol, market in active_keys]
        conn.execute(
            f"""
            UPDATE market_watchlist
            SET active=0, updated_at=?
            WHERE active=1
              AND phase <> 'MOMENTUM_OBSERVE'
              AND phase <> 'MOMENTUM_OBSERVE'
              AND symbol || '|' || market NOT IN ({placeholders})
            """,
            [now_s, *flat],
        )
    else:
        conn.execute(
            """
            UPDATE market_watchlist
            SET active=0, updated_at=?
            WHERE active=1
            """,
            (now_s,),
        )

    conn.commit()
    conn.close()


def collect_tickers(settings: Settings, tickers_by_market: dict[str, list[dict[str, Any]]]) -> list[MarketTicker]:
    quote = settings.quote_coin.upper()

    allow = set(settings.symbols_allowlist or [])
    block = set(settings.symbols_blocklist or [])

    extra_block = set(env_csv("WATCHLIST_SYMBOL_BLOCKLIST", "BTCUSDT,XAUUSDT,XAUTUSDT"))
    block |= extra_block

    patterns = list(settings.symbol_exclude_patterns or [])
    patterns += env_csv(
        "WATCHLIST_SYMBOL_EXCLUDE_PATTERNS",
        "1000*,*TEST*,*DEMO*,*USDC,*PERP",
    )

    min_turnover = env_float("WATCHLIST_MIN_TURNOVER_24H", env_float("ACC_MIN_NOTIONAL_24H", 3_000_000))
    min_price = env_float("WATCHLIST_MIN_LAST_PRICE", settings.min_last_price)

    out: list[MarketTicker] = []

    for market, rows in tickers_by_market.items():
        for row in rows:
            symbol = str(row.get("symbol") or "").upper()
            if not symbol.endswith(quote):
                continue
            if allow and symbol not in allow:
                continue
            if symbol in block:
                continue
            if matches_any(symbol, patterns):
                continue

            last_price = safe_float(row.get("lastPrice"))
            turnover24h = safe_float(row.get("turnover24h"))
            price24h_pct = safe_float(row.get("price24hPcnt")) * 100.0

            if last_price < min_price:
                continue
            if turnover24h < min_turnover:
                continue

            out.append(
                MarketTicker(
                    symbol=symbol,
                    market=market,
                    last_price=last_price,
                    turnover24h=turnover24h,
                    price24h_pct=price24h_pct,
                )
            )

    out.sort(key=lambda x: x.turnover24h, reverse=True)
    return out


def avg(values) -> float:
    values = [safe_float(v) for v in values if v is not None]
    return mean(values) if values else 0.0


def evaluate_accumulation(ticker: MarketTicker, df) -> WatchCandidate | None:
    if df is None or df.empty:
        return None

    min_bars = env_int("WATCHLIST_MIN_BARS", 36)
    if len(df) < min_bars:
        return None

    base_bars = env_int("WATCHLIST_BASE_BARS", 24)
    volume_recent_bars = env_int("WATCHLIST_VOLUME_RECENT_BARS", 6)
    volume_base_bars = env_int("WATCHLIST_VOLUME_BASE_BARS", 24)

    max_base_range_pct = env_float("WATCHLIST_MAX_BASE_RANGE_PCT", 12.0)
    max_recent_impulse_pct = env_float("WATCHLIST_MAX_RECENT_IMPULSE_PCT", 10.0)
    max_late_24h_pct = env_float("WATCHLIST_MAX_LATE_24H_PCT", 35.0)
    min_score = env_float("WATCHLIST_MIN_SCORE", 5.0)

    tail = df.tail(base_bars)
    recent = df.tail(volume_recent_bars)
    prev_volume = df.tail(volume_base_bars + volume_recent_bars).head(volume_base_bars)

    close = safe_float(df["close"].iloc[-1])
    if close <= 0:
        return None

    high = safe_float(tail["high"].max())
    low = safe_float(tail["low"].min())
    if high <= 0 or low <= 0 or high <= low:
        return None

    base_range_pct = ((high - low) / close) * 100.0
    close_pos = (close - low) / max(high - low, 1e-12)

    volume_recent = avg(recent["volume"].tolist())
    volume_base = avg(prev_volume["volume"].tolist())
    volume_expansion = volume_recent / max(volume_base, 1e-12)

    turnover_recent = avg(recent["turnover"].tolist()) if "turnover" in df.columns else 0.0

    close_3 = safe_float(df["close"].iloc[-4]) if len(df) >= 4 else close
    close_6 = safe_float(df["close"].iloc[-7]) if len(df) >= 7 else close
    close_12 = safe_float(df["close"].iloc[-13]) if len(df) >= 13 else close

    move_3h_pct = ((close / close_3) - 1.0) * 100.0 if close_3 > 0 else 0.0
    move_6h_pct = ((close / close_6) - 1.0) * 100.0 if close_6 > 0 else 0.0
    move_12h_pct = ((close / close_12) - 1.0) * 100.0 if close_12 > 0 else 0.0

    abs_recent_impulse = max(abs(move_3h_pct), abs(move_6h_pct))

    range_score = max(0.0, (max_base_range_pct - base_range_pct) / max(max_base_range_pct, 1e-9)) * 3.0

    if 0.45 <= close_pos <= 0.86:
        close_pos_score = 2.0
    elif 0.30 <= close_pos <= 0.92:
        close_pos_score = 1.0
    else:
        close_pos_score = 0.0

    volume_score = min(max(volume_expansion, 0.0), 3.0) / 3.0 * 2.0

    turnover_score = 0.0
    min_turnover = env_float("WATCHLIST_MIN_TURNOVER_24H", 3_000_000)
    if ticker.turnover24h > min_turnover:
        turnover_score = min(math.log10(ticker.turnover24h / max(min_turnover, 1.0)), 1.0) * 2.0

    momentum_seed_score = 0.0
    if 1.0 <= ticker.price24h_pct <= max_late_24h_pct:
        momentum_seed_score = 1.0
    elif -4.0 <= ticker.price24h_pct < 1.0:
        momentum_seed_score = 0.5

    late_penalty = 0.0
    late_reasons: list[str] = []

    if ticker.price24h_pct > max_late_24h_pct:
        late_penalty += 1.5
        late_reasons.append("late_24h_pump")

    if abs_recent_impulse > max_recent_impulse_pct:
        late_penalty += 1.0
        late_reasons.append("recent_impulse_too_large")

    score = range_score + close_pos_score + volume_score + turnover_score + momentum_seed_score - late_penalty
    score = round(max(score, 0.0), 3)

    reasons: list[str] = []

    if base_range_pct <= max_base_range_pct:
        reasons.append("compressed_base")
    if 0.30 <= close_pos <= 0.92:
        reasons.append("price_inside_base")
    if volume_expansion >= 1.15:
        reasons.append("volume_expansion")
    if ticker.price24h_pct >= 3.0:
        reasons.append("positive_24h_momentum")
    if move_6h_pct > 0:
        reasons.append("positive_6h_momentum")
    reasons.extend(late_reasons)

    if score < min_score:
        return None

    phase = "ACCUMULATION"
    trade_eligible = True

    wide_base_observe_only = base_range_pct > max_base_range_pct
    hot_mover_observe_only = ticker.price24h_pct >= env_float("WATCHLIST_HOT_MOVER_24H_PCT", 8.0)

    if volume_expansion >= 1.5 and close_pos >= 0.60 and abs_recent_impulse <= max_recent_impulse_pct:
        phase = "PRE_IMPULSE"

    if wide_base_observe_only and hot_mover_observe_only:
        phase = "HOT_MOVER_OBSERVE_ONLY"
        trade_eligible = False
    elif wide_base_observe_only:
        phase = "WIDE_RANGE_OBSERVE_ONLY"
        trade_eligible = False

    if late_reasons:
        phase = "LATE_IMPULSE_OBSERVE_ONLY"
        trade_eligible = False

    metrics = {
        "last_price": close,
        "turnover24h": ticker.turnover24h,
        "price24h_pct": ticker.price24h_pct,
        "base_range_pct": base_range_pct,
        "close_pos": close_pos,
        "volume_expansion": volume_expansion,
        "turnover_recent_avg": turnover_recent,
        "move_3h_pct": move_3h_pct,
        "move_6h_pct": move_6h_pct,
        "move_12h_pct": move_12h_pct,
        "range_score": range_score,
        "close_pos_score": close_pos_score,
        "volume_score": volume_score,
        "turnover_score": turnover_score,
        "momentum_seed_score": momentum_seed_score,
        "late_penalty": late_penalty,
    }

    return WatchCandidate(
        symbol=ticker.symbol,
        market=ticker.market,
        phase=phase,
        score=score,
        trade_eligible=trade_eligible,
        reason=",".join(reasons) if reasons else "watchlist_candidate",
        metrics=metrics,
    )


async def analyze_one(rest: BybitRestClient, sem: asyncio.Semaphore, ticker: MarketTicker) -> WatchCandidate | None:
    interval = os.getenv("WATCHLIST_KLINE_INTERVAL", "60").strip() or "60"
    limit = env_int("WATCHLIST_KLINE_LIMIT", 96)

    async with sem:
        try:
            df = await rest.fetch_klines(
                ticker.symbol,
                interval=interval,
                limit=limit,
                category=ticker.market,
            )
        except Exception:
            return None

    return evaluate_accumulation(ticker, df)


def write_reports(candidates: list[WatchCandidate]) -> None:
    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    json_path = reports_dir / "market_watchlist_latest.json"
    csv_path = reports_dir / "market_watchlist_latest.csv"

    payload = []
    for c in candidates:
        payload.append(
            {
                "symbol": c.symbol,
                "market": c.market,
                "phase": c.phase,
                "score": c.score,
                "trade_eligible": c.trade_eligible,
                "reason": c.reason,
                "metrics": c.metrics,
            }
        )

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "symbol",
                "market",
                "phase",
                "score",
                "trade_eligible",
                "price24h_pct",
                "turnover24h",
                "base_range_pct",
                "close_pos",
                "volume_expansion",
                "move_3h_pct",
                "move_6h_pct",
                "reason",
            ]
        )
        for c in candidates:
            m = c.metrics
            writer.writerow(
                [
                    c.symbol,
                    c.market,
                    c.phase,
                    c.score,
                    int(c.trade_eligible),
                    round(float(m.get("price24h_pct") or 0), 3),
                    round(float(m.get("turnover24h") or 0), 2),
                    round(float(m.get("base_range_pct") or 0), 3),
                    round(float(m.get("close_pos") or 0), 3),
                    round(float(m.get("volume_expansion") or 0), 3),
                    round(float(m.get("move_3h_pct") or 0), 3),
                    round(float(m.get("move_6h_pct") or 0), 3),
                    c.reason,
                ]
            )

    print(f"\nReports written:")
    print(f"  {json_path}")
    print(f"  {csv_path}")


async def main() -> None:
    settings = Settings()

    db_path = os.getenv("SIGNALS_DB_PATH", "data/signals.db")
    ttl_hours = env_float("WATCHLIST_TTL_HOURS", 12.0)
    concurrency = max(1, env_int("WATCHLIST_REBUILD_CONCURRENCY", 8))

    categories = [str(x).lower() for x in settings.market_categories if str(x).strip()]
    if not categories:
        categories = ["linear"]

    print("=== MARKET WATCHLIST REBUILD ===")
    print("mode: report-only")
    print("db:", db_path)
    print("categories:", categories)
    print("ttl_hours:", ttl_hours)
    print("concurrency:", concurrency)

    async with BybitRestClient(
        settings.rest_base_url,
        timeout_seconds=settings.rest_timeout_seconds,
        retries=settings.rest_retries,
    ) as rest:
        tickers_by_market: dict[str, list[dict[str, Any]]] = {}
        for category in categories:
            rows = await rest.fetch_tickers(category=category)
            tickers_by_market[category] = rows

        tickers = collect_tickers(settings, tickers_by_market)

        max_symbols = env_int("WATCHLIST_FULL_SCAN_MAX_SYMBOLS", 0)
        if max_symbols > 0:
            tickers = tickers[:max_symbols]

        print("tickers_after_filter:", len(tickers))

        sem = asyncio.Semaphore(concurrency)
        tasks = [analyze_one(rest, sem, t) for t in tickers]
        raw = await asyncio.gather(*tasks)

    candidates = [c for c in raw if c is not None]
    candidates.sort(key=lambda c: c.score, reverse=True)

    upsert_candidates(db_path, candidates, ttl_hours)
    write_reports(candidates)

    print("\n=== RESULT ===")
    print("checked:", len(tickers))
    print("candidates:", len(candidates))
    print("trade_eligible:", sum(1 for c in candidates if c.trade_eligible))
    print("observe_only:", sum(1 for c in candidates if not c.trade_eligible))

    print("\n=== TOP 40 WATCHLIST ===")
    for c in candidates[:40]:
        m = c.metrics
        print(
            f"{c.symbol:14} {c.market:7} {c.phase:26} "
            f"score={c.score:5.2f} eligible={int(c.trade_eligible)} "
            f"24h={float(m.get('price24h_pct') or 0):7.2f}% "
            f"range={float(m.get('base_range_pct') or 0):6.2f}% "
            f"volx={float(m.get('volume_expansion') or 0):5.2f} "
            f"move6h={float(m.get('move_6h_pct') or 0):7.2f}% "
            f"reason={c.reason}"
        )


if __name__ == "__main__":
    asyncio.run(main())
