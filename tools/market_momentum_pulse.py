from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orderflow_accum.bybit_rest import BybitRestClient
from orderflow_accum.config import Settings
from market_watchlist_rebuild import (
    collect_tickers,
    ensure_schema,
    env_float,
    iso,
    utc_now,
)

PHASE = "MOMENTUM_OBSERVE"


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(float(str(raw).strip()))
    except ValueError:
        return default


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def ensure_pulse_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_pulse_snapshots (
            symbol TEXT NOT NULL,
            market TEXT NOT NULL,
            last_price REAL NOT NULL,
            turnover24h REAL NOT NULL,
            price24h_pct REAL NOT NULL,
            captured_at TEXT NOT NULL,
            PRIMARY KEY(symbol, market)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_pulse_captured
        ON market_pulse_snapshots(captured_at)
        """
    )
    conn.commit()


def upsert_candidate(
    candidates: dict[tuple[str, str], dict],
    candidate: dict,
) -> None:
    key = (candidate["symbol"], candidate["market"])
    previous = candidates.get(key)

    if previous is None or float(candidate["score"]) > float(previous["score"]):
        candidates[key] = candidate


async def main() -> None:
    settings = Settings()
    db_path = os.getenv("SIGNALS_DB_PATH", "data/signals.db")
    now = utc_now()
    now_s = iso(now)

    # Свежий импульс за последние ~5 минут.
    min_move_pct = env_float("PULSE_MIN_MOVE_5M_PCT", 0.15)
    max_move_pct = env_float("PULSE_MAX_MOVE_5M_PCT", 3.50)
    min_turnover_delta = env_float("PULSE_MIN_TURNOVER_DELTA_USDT", 25000.0)
    max_sample_age = env_float("PULSE_MAX_SAMPLE_AGE_MINUTES", 15.0)
    fast_min_24h = env_float("PULSE_FAST_MIN_24H_PCT", -3.0)

    # Роллинг-наблюдение: монета уже начала движение, но ещё не считается поздним пампом.
    rolling_enabled = env_bool("PULSE_ROLLING_ENABLED", True)
    rolling_min_24h = env_float("PULSE_ROLLING_MIN_24H_PCT", 2.0)
    rolling_max_24h = env_float("PULSE_ROLLING_MAX_24H_PCT", 25.0)
    rolling_min_turnover = env_float("PULSE_ROLLING_MIN_TURNOVER_24H", 3000000.0)

    ttl_minutes = env_float("PULSE_TTL_MINUTES", 90.0)
    max_observe_symbols = max(env_int("PULSE_MAX_OBSERVE_SYMBOLS", 35), 1)

    categories = [
        str(item).lower()
        for item in settings.market_categories
        if str(item).strip()
    ] or ["linear"]

    async with BybitRestClient(
        settings.rest_base_url,
        timeout_seconds=settings.rest_timeout_seconds,
        retries=settings.rest_retries,
    ) as rest:
        tickers_by_market = {}
        for category in categories:
            tickers_by_market[category] = await rest.fetch_tickers(category=category)

    tickers = collect_tickers(settings, tickers_by_market)

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    ensure_schema(conn)
    ensure_pulse_schema(conn)

    previous = {
        (row["symbol"], row["market"]): row
        for row in conn.execute(
            """
            SELECT symbol, market, last_price, turnover24h, captured_at
            FROM market_pulse_snapshots
            """
        ).fetchall()
    }

    candidates: dict[tuple[str, str], dict] = {}
    fast_count = 0
    rolling_count = 0

    for ticker in tickers:
        prev = previous.get((ticker.symbol, ticker.market))

        # 1) Fast 5m pulse.
        if prev is not None:
            previous_price = float(prev["last_price"] or 0.0)
            previous_turnover = float(prev["turnover24h"] or 0.0)
            previous_time = parse_time(prev["captured_at"])

            if previous_price > 0 and previous_time is not None:
                age_minutes = (now - previous_time).total_seconds() / 60.0
                move_pct = ((ticker.last_price / previous_price) - 1.0) * 100.0
                turnover_delta = max(ticker.turnover24h - previous_turnover, 0.0)

                fast_ok = (
                    0 < age_minutes <= max_sample_age
                    and min_move_pct <= move_pct <= max_move_pct
                    and turnover_delta >= min_turnover_delta
                    and fast_min_24h <= ticker.price24h_pct <= rolling_max_24h
                )

                if fast_ok:
                    move_score = min(move_pct / max(min_move_pct, 0.01), 4.0)
                    turnover_score = min(
                        turnover_delta / max(min_turnover_delta, 1.0),
                        4.0,
                    )

                    upsert_candidate(
                        candidates,
                        {
                            "symbol": ticker.symbol,
                            "market": ticker.market,
                            "score": round(7.0 + move_score * 0.75 + turnover_score * 0.50, 3),
                            "source": "fast_5m_pulse",
                            "reason": "pulse_5m_up,pulse_turnover_delta",
                            "last_price": ticker.last_price,
                            "turnover24h": ticker.turnover24h,
                            "price24h_pct": ticker.price24h_pct,
                            "move_5m_pct": move_pct,
                            "turnover_delta": turnover_delta,
                            "sample_age_minutes": age_minutes,
                        },
                    )
                    fast_count += 1

        # 2) Rolling 24h momentum observe.
        # Не вход. Только добавление монеты в realtime scanner.
        if rolling_enabled:
            rolling_ok = (
                rolling_min_24h <= ticker.price24h_pct <= rolling_max_24h
                and ticker.turnover24h >= rolling_min_turnover
            )

            if rolling_ok:
                momentum_score = min(
                    ticker.price24h_pct / max(rolling_min_24h, 0.01),
                    6.0,
                )
                liquidity_score = min(
                    math.log10(
                        max(ticker.turnover24h, rolling_min_turnover)
                        / max(rolling_min_turnover, 1.0)
                    ),
                    2.0,
                )

                upsert_candidate(
                    candidates,
                    {
                        "symbol": ticker.symbol,
                        "market": ticker.market,
                        "score": round(5.0 + momentum_score * 0.70 + liquidity_score, 3),
                        "source": "rolling_24h_momentum",
                        "reason": "rolling_24h_momentum,liquid_alt,observe_only",
                        "last_price": ticker.last_price,
                        "turnover24h": ticker.turnover24h,
                        "price24h_pct": ticker.price24h_pct,
                        "move_5m_pct": None,
                        "turnover_delta": None,
                        "sample_age_minutes": None,
                    },
                )
                rolling_count += 1

        conn.execute(
            """
            INSERT INTO market_pulse_snapshots (
                symbol, market, last_price, turnover24h, price24h_pct, captured_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, market) DO UPDATE SET
                last_price=excluded.last_price,
                turnover24h=excluded.turnover24h,
                price24h_pct=excluded.price24h_pct,
                captured_at=excluded.captured_at
            """,
            (
                ticker.symbol,
                ticker.market,
                ticker.last_price,
                ticker.turnover24h,
                ticker.price24h_pct,
                now_s,
            ),
        )

    selected = sorted(
        candidates.values(),
        key=lambda item: float(item["score"]),
        reverse=True,
    )[:max_observe_symbols]

    expires_s = iso(now + timedelta(minutes=max(ttl_minutes, 5.0)))
    persisted = 0
    active_keys: list[str] = []

    for item in selected:
        existing = conn.execute(
            """
            SELECT phase, trade_eligible
            FROM market_watchlist
            WHERE symbol=? AND market=?
            """,
            (item["symbol"], item["market"]),
        ).fetchone()

        # Нельзя затирать базовый trade-eligible setup momentum-слоем.
        if existing is not None and int(existing["trade_eligible"] or 0) == 1:
            continue

        metrics = {
            "last_price": item["last_price"],
            "turnover24h": item["turnover24h"],
            "price24h_pct": item["price24h_pct"],
            "pulse_source": item["source"],
            "pulse_move_5m_pct": item["move_5m_pct"],
            "pulse_turnover_delta": item["turnover_delta"],
            "pulse_sample_age_minutes": item["sample_age_minutes"],
        }

        conn.execute(
            """
            INSERT INTO market_watchlist (
                symbol, market, phase, score, trade_eligible, active,
                reason, metrics_json, discovered_at, last_seen_at, expires_at, updated_at
            )
            VALUES (?, ?, ?, ?, 0, 1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, market) DO UPDATE SET
                phase=excluded.phase,
                score=excluded.score,
                trade_eligible=0,
                active=1,
                reason=excluded.reason,
                metrics_json=excluded.metrics_json,
                last_seen_at=excluded.last_seen_at,
                expires_at=excluded.expires_at,
                updated_at=excluded.updated_at
            """,
            (
                item["symbol"],
                item["market"],
                PHASE,
                item["score"],
                item["reason"],
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                now_s,
                now_s,
                expires_s,
                now_s,
            ),
        )

        active_keys.append(f"{item['symbol']}|{item['market']}")
        persisted += 1

    # В активном momentum-слое оставляем только актуальную выборку.
    if active_keys:
        placeholders = ",".join("?" for _ in active_keys)
        conn.execute(
            f"""
            UPDATE market_watchlist
            SET active=0, updated_at=?
            WHERE active=1
              AND phase=?
              AND symbol || '|' || market NOT IN ({placeholders})
            """,
            [now_s, PHASE, *active_keys],
        )
    else:
        conn.execute(
            """
            UPDATE market_watchlist
            SET active=0, updated_at=?
            WHERE active=1
              AND phase=?
            """,
            (now_s, PHASE),
        )

    conn.commit()
    conn.close()

    print("=== MARKET MOMENTUM PULSE ===")
    print("tickers_after_filter:", len(tickers))
    print("fast_5m_candidates:", fast_count)
    print("rolling_24h_candidates:", rolling_count)
    print("selected_momentum_observe:", len(selected))
    print("persisted_momentum_observe:", persisted)

    for item in selected:
        move_text = (
            f"{float(item['move_5m_pct']):6.2f}%"
            if item["move_5m_pct"] is not None
            else "   n/a"
        )

        print(
            f"{item['symbol']:14} "
            f"source={item['source']:22} "
            f"score={float(item['score']):5.2f} "
            f"move5m={move_text} "
            f"24h={float(item['price24h_pct']):6.2f}% "
            f"turnover={float(item['turnover24h']):13,.0f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
