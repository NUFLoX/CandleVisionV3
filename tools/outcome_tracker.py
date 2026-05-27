from __future__ import annotations

import argparse
import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ACTIVE_STATUSES = {
    "WATCHING",
    "ACCUMULATION",
    "PRE_IMPULSE",
    "BREAKOUT_PRESSURE",
    "DISTRIBUTION",
    "PRE_DUMP",
    "PENDING",
}


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt
    except Exception:
        return None


def _interval_to_minutes(tf: str) -> int:
    t = (tf or "1").strip().upper()

    mapping = {
        "1": 1,
        "3": 3,
        "5": 5,
        "15": 15,
        "30": 30,
        "60": 60,
        "120": 120,
        "240": 240,
        "D": 1440,
    }

    return mapping.get(t, 1)


@dataclass(slots=True)
class Outcome:
    status: str
    bars_checked: int
    time_to_tp1_minutes: float | None
    time_to_tp2_minutes: float | None
    time_to_sl_minutes: float | None
    max_gain_pct: float
    max_drawdown_pct: float


def evaluate_outcome(
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    rows: list[dict],
    interval_min: int,
) -> Outcome:
    return evaluate_outcome_for_side(
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        rows=rows,
        interval_min=interval_min,
        side="Buy",
    )


def evaluate_outcome_for_side(
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    rows: list[dict],
    interval_min: int,
    side: str = "Buy",
) -> Outcome:
    max_gain = float("-inf")
    max_dd = float("inf")
    t_tp1 = None
    t_tp2 = None
    t_sl = None

    is_short = str(side or "Buy").lower() == "sell"

    for idx, candle in enumerate(rows):
        high = float(candle["high"])
        low = float(candle["low"])

        if is_short:
            gain = (entry - low) / max(entry, 1e-12) * 100.0
            dd = (entry - high) / max(entry, 1e-12) * 100.0

            hit_tp1 = low <= tp1
            hit_tp2 = low <= tp2
            hit_sl = high >= sl
        else:
            gain = (high - entry) / max(entry, 1e-12) * 100.0
            dd = (low - entry) / max(entry, 1e-12) * 100.0

            hit_tp1 = high >= tp1
            hit_tp2 = high >= tp2
            hit_sl = low <= sl

        max_gain = max(max_gain, gain)
        max_dd = min(max_dd, dd)

        if hit_tp1 and t_tp1 is None:
            t_tp1 = (idx + 1) * interval_min

        if hit_tp2 and t_tp2 is None:
            t_tp2 = (idx + 1) * interval_min

        if hit_sl and t_sl is None:
            t_sl = (idx + 1) * interval_min

        if hit_sl and (hit_tp1 or hit_tp2):
            return Outcome("AMBIGUOUS", idx + 1, t_tp1, t_tp2, t_sl, max_gain, max_dd)

        if hit_sl:
            return Outcome("SL", idx + 1, t_tp1, t_tp2, t_sl, max_gain, max_dd)

        if hit_tp2:
            return Outcome("TP2", idx + 1, t_tp1, t_tp2, t_sl, max_gain, max_dd)

    safe_gain = max_gain if max_gain != float("-inf") else 0.0
    safe_dd = max_dd if max_dd != float("inf") else 0.0

    if t_tp1 is not None:
        return Outcome("TP1", len(rows), t_tp1, t_tp2, t_sl, safe_gain, safe_dd)

    return Outcome("PENDING", len(rows), t_tp1, t_tp2, t_sl, safe_gain, safe_dd)


async def run_once(db_path: str, lookahead_bars: int, expires_hours: int) -> int:
    from orderflow_accum.bybit_rest import BybitRestClient
    from orderflow_accum.config import Settings
    from orderflow_accum.signal_store import SignalStore

    settings = Settings()
    store = SignalStore(db_path=db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(ACTIVE_STATUSES))
    rows = conn.execute(
        f"SELECT * FROM signals WHERE status IN ({placeholders})",
        tuple(ACTIVE_STATUSES),
    ).fetchall()

    updated = 0

    async with BybitRestClient(
        settings.rest_base_url,
        timeout_seconds=settings.rest_timeout_seconds,
        retries=settings.rest_retries,
    ) as client:
        for row in rows:
            tf = str(row["timeframe"] or "1")
            interval_min = _interval_to_minutes(tf)

            created = _parse_time(row["first_seen"]) or datetime.now(timezone.utc)
            age_min = (datetime.now(timezone.utc) - created).total_seconds() / 60.0

            if age_min > expires_hours * 60:
                prev_status = str(row["status"] or "PENDING")

                conn.execute(
                    """
                    UPDATE signals
                    SET status='EXPIRED',
                        outcome='EXPIRED',
                        outcome_checked_at=?
                    WHERE id=?
                    """,
                    (
                        datetime.now(timezone.utc).isoformat(),
                        row["id"],
                    ),
                )

                if prev_status != "EXPIRED":
                    store.add_event(
                        signal_key=str(row["signal_key"]),
                        symbol=str(row["symbol"]),
                        timeframe=str(row["timeframe"]),
                        event_type="outcome_transition",
                        from_status=prev_status,
                        to_status="EXPIRED",
                        score_last=float(row["score_last"] or 0.0),
                    )

                updated += 1
                continue

            category = str(row["market"] or "linear").lower()

            df = await client.fetch_klines(
                str(row["symbol"]),
                interval=tf,
                limit=lookahead_bars,
                category=category,
            )

            if df.empty:
                continue

            candles = df.to_dict("records")

            outcome = evaluate_outcome_for_side(
                float(row["entry"]),
                float(row["stop_loss"]),
                float(row["take_profit_1"]),
                float(row["take_profit_2"]),
                candles,
                interval_min,
                side=str(row["side"] or "Buy"),
            )

            prev_status = str(row["status"] or "PENDING")
            status = prev_status

            if outcome.status in {"TP1", "TP2", "SL", "AMBIGUOUS"}:
                status = outcome.status

            conn.execute(
                """
                UPDATE signals
                SET
                    status=?,
                    outcome=?,
                    outcome_checked_at=?,
                    time_to_tp1_minutes=?,
                    time_to_tp2_minutes=?,
                    time_to_sl_minutes=?,
                    max_gain_pct=?,
                    max_drawdown_pct=?
                WHERE id=?
                """,
                (
                    status,
                    outcome.status,
                    datetime.now(timezone.utc).isoformat(),
                    outcome.time_to_tp1_minutes,
                    outcome.time_to_tp2_minutes,
                    outcome.time_to_sl_minutes,
                    outcome.max_gain_pct,
                    outcome.max_drawdown_pct,
                    row["id"],
                ),
            )

            if status != prev_status:
                store.add_event(
                    signal_key=str(row["signal_key"]),
                    symbol=str(row["symbol"]),
                    timeframe=str(row["timeframe"]),
                    event_type="outcome_transition",
                    from_status=prev_status,
                    to_status=status,
                    score_last=float(row["score_last"] or 0.0),
                )

            updated += 1

    conn.commit()
    conn.close()
    store.close()

    return updated


async def main() -> None:
    parser = argparse.ArgumentParser(description="Track outcomes for active signals in signals.db")
    parser.add_argument("--db", default="data/signals.db")
    parser.add_argument("--lookahead-bars", type=int, default=180)
    parser.add_argument("--expires-hours", type=int, default=48)
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval-minutes", type=int, default=10, help="Loop interval in minutes when --loop is set")

    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"signals db not found: {args.db}")
        return

    if not args.loop:
        count = await run_once(args.db, args.lookahead_bars, args.expires_hours)
        print(f"updated={count}")
        return

    interval_seconds = max(args.interval_minutes, 1) * 60

    while True:
        count = await run_once(args.db, args.lookahead_bars, args.expires_hours)
        print(f"updated={count}")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(main())

