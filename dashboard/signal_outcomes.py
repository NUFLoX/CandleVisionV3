from __future__ import annotations

import asyncio
import os
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Literal

from .schemas import Signal, SignalOutcome, SignalStatsSummary

OutcomeStatus = Literal["tp", "sl", "ambiguous", "expired"]

DEFAULT_DB_PATH = "data/signal_stats.db"
DEFAULT_BYBIT_BASE_URL = "https://api.bybit.com"
INTERVAL_MS = {
    "1": 60_000,
    "3": 3 * 60_000,
    "5": 5 * 60_000,
    "15": 15 * 60_000,
    "30": 30 * 60_000,
    "60": 60 * 60_000,
    "120": 120 * 60_000,
    "240": 240 * 60_000,
    "D": 24 * 60 * 60_000,
    "W": 7 * 24 * 60 * 60_000,
}


def signal_stats_db_path() -> Path:
    return Path(os.getenv("DASHBOARD_SIGNAL_STATS_DB", DEFAULT_DB_PATH))


def normalize_interval(timeframe: str) -> str:
    value = str(timeframe or "60").strip().lower()
    aliases = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "60m": "60",
        "2h": "120",
        "4h": "240",
        "1d": "D",
        "d": "D",
        "1w": "W",
        "w": "W",
    }
    return aliases.get(value, value.upper() if value in {"d", "w"} else value)


def interval_milliseconds(timeframe: str) -> int:
    return INTERVAL_MS.get(normalize_interval(timeframe), 60 * 60_000)


def infer_direction(signal: Signal) -> str:
    if signal.take_profit_1 < signal.entry and signal.stop_loss > signal.entry:
        return "short"
    return "long"


def _as_float(row: Any, key: str, index: int | None = None) -> float:
    if isinstance(row, dict):
        return float(row[key])
    if index is not None:
        return float(row[index])
    return float(getattr(row, key))


def _as_int(row: Any, key: str, index: int | None = None) -> int | None:
    try:
        if isinstance(row, dict):
            value = row.get(key)
        elif index is not None:
            value = row[index]
        else:
            value = getattr(row, key)
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _row_close_time(row: Any, default: datetime, interval_ms: int) -> datetime:
    start = _as_int(row, "start", 0) or _as_int(row, "time", None)
    if start is None:
        return default
    if start < 10_000_000_000:
        start *= 1000
    return datetime.fromtimestamp((start + interval_ms) / 1000, tz=timezone.utc)


def _risk(signal: Signal) -> float:
    return abs(signal.entry - signal.stop_loss)


def calculate_r(signal: Signal, exit_price: float) -> float:
    risk = _risk(signal)
    if risk <= 0:
        return 0.0
    direction = infer_direction(signal)
    if direction == "short":
        return (signal.entry - exit_price) / risk
    return (exit_price - signal.entry) / risk


def calculate_signal_outcome(
    signal: Signal,
    candles: Iterable[Any],
    *,
    max_bars: int = 120,
    now: datetime | None = None,
) -> SignalOutcome:
    """Classify a signal by scanning chronological candles after signal creation."""

    checked_at = now or datetime.now(timezone.utc)
    direction = infer_direction(signal)
    risk = _risk(signal)
    created_at = signal.created_at if signal.created_at.tzinfo else signal.created_at.replace(tzinfo=timezone.utc)
    interval_ms = interval_milliseconds(signal.timeframe)
    rows = list(candles)[:max_bars]
    last_close = signal.entry
    last_time = created_at

    for index, row in enumerate(rows, start=1):
        high = _as_float(row, "high", 2)
        low = _as_float(row, "low", 3)
        close = _as_float(row, "close", 4)
        last_close = close
        last_time = _row_close_time(row, created_at + timedelta(milliseconds=interval_ms * index), interval_ms)

        if direction == "short":
            hit_tp = low <= signal.take_profit_1
            hit_sl = high >= signal.stop_loss
        else:
            hit_tp = high >= signal.take_profit_1
            hit_sl = low <= signal.stop_loss

        if hit_tp and hit_sl:
            return _build_outcome(signal, "ambiguous", direction, 0.0, index, last_time, checked_at)
        if hit_tp:
            r_multiple = abs(signal.take_profit_1 - signal.entry) / risk if risk > 0 else 0.0
            return _build_outcome(signal, "tp", direction, r_multiple, index, last_time, checked_at)
        if hit_sl:
            return _build_outcome(signal, "sl", direction, -1.0, index, last_time, checked_at)

    r_multiple = calculate_r(signal, last_close) if rows else 0.0
    return _build_outcome(signal, "expired", direction, r_multiple, len(rows), last_time, checked_at)


def _build_outcome(
    signal: Signal,
    status: OutcomeStatus,
    direction: str,
    r_multiple: float,
    bars_checked: int,
    closed_at: datetime,
    checked_at: datetime,
) -> SignalOutcome:
    return SignalOutcome(
        signal_id=signal.id,
        symbol=signal.symbol,
        exchange=signal.exchange,
        timeframe=signal.timeframe,
        signal_type=signal.signal_type.value,
        direction=direction,
        entry=signal.entry,
        stop_loss=signal.stop_loss,
        take_profit_1=signal.take_profit_1,
        outcome=status,
        r_multiple=round(r_multiple, 4),
        bars_checked=bars_checked,
        created_at=signal.created_at,
        closed_at=closed_at,
        checked_at=checked_at,
    )


class SignalOutcomeStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or signal_stats_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_outcomes (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    take_profit_1 REAL NOT NULL,
                    outcome TEXT NOT NULL,
                    r_multiple REAL NOT NULL,
                    bars_checked INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    closed_at TEXT NOT NULL,
                    checked_at TEXT NOT NULL
                )
                """
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_signal_outcomes_symbol ON signal_outcomes(symbol)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_signal_outcomes_timeframe ON signal_outcomes(timeframe)")

    def upsert_many(self, outcomes: Iterable[SignalOutcome]) -> None:
        rows = [outcome.model_dump(mode="json") if hasattr(outcome, "model_dump") else outcome.dict() for outcome in outcomes]
        if not rows:
            return
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO signal_outcomes (
                    signal_id, symbol, exchange, timeframe, signal_type, direction, entry, stop_loss, take_profit_1,
                    outcome, r_multiple, bars_checked, created_at, closed_at, checked_at
                ) VALUES (
                    :signal_id, :symbol, :exchange, :timeframe, :signal_type, :direction, :entry, :stop_loss,
                    :take_profit_1, :outcome, :r_multiple, :bars_checked, :created_at, :closed_at, :checked_at
                )
                ON CONFLICT(signal_id) DO UPDATE SET
                    symbol=excluded.symbol,
                    exchange=excluded.exchange,
                    timeframe=excluded.timeframe,
                    signal_type=excluded.signal_type,
                    direction=excluded.direction,
                    entry=excluded.entry,
                    stop_loss=excluded.stop_loss,
                    take_profit_1=excluded.take_profit_1,
                    outcome=excluded.outcome,
                    r_multiple=excluded.r_multiple,
                    bars_checked=excluded.bars_checked,
                    created_at=excluded.created_at,
                    closed_at=excluded.closed_at,
                    checked_at=excluded.checked_at
                """,
                rows,
            )

    def list_outcomes(self, limit: int = 500) -> list[SignalOutcome]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM signal_outcomes ORDER BY checked_at DESC, created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [SignalOutcome(**dict(row)) for row in rows]

    def stats(self) -> SignalStatsSummary:
        return aggregate_signal_stats(self.list_outcomes(limit=10_000))


def aggregate_signal_stats(outcomes: Iterable[SignalOutcome]) -> SignalStatsSummary:
    items = list(outcomes)
    total = len(items)
    wins = sum(1 for item in items if item.outcome == "tp")
    losses = sum(1 for item in items if item.outcome == "sl")
    ambiguous = sum(1 for item in items if item.outcome == "ambiguous")
    expired = sum(1 for item in items if item.outcome == "expired")
    decided = wins + losses
    total_r = sum(item.r_multiple for item in items)

    def breakdown(key: str) -> list[dict[str, Any]]:
        groups: dict[str, list[SignalOutcome]] = defaultdict(list)
        for item in items:
            groups[str(getattr(item, key))].append(item)
        rows: list[dict[str, Any]] = []
        for value, group in sorted(groups.items()):
            group_wins = sum(1 for item in group if item.outcome == "tp")
            group_losses = sum(1 for item in group if item.outcome == "sl")
            group_decided = group_wins + group_losses
            group_r = sum(item.r_multiple for item in group)
            rows.append(
                {
                    key: value,
                    "total": len(group),
                    "wins": group_wins,
                    "losses": group_losses,
                    "win_rate": round(group_wins / group_decided, 4) if group_decided else 0.0,
                    "avg_r": round(group_r / len(group), 4) if group else 0.0,
                    "total_r": round(group_r, 4),
                }
            )
        return rows

    return SignalStatsSummary(
        total=total,
        wins=wins,
        losses=losses,
        ambiguous=ambiguous,
        expired=expired,
        win_rate=round(wins / decided, 4) if decided else 0.0,
        avg_r=round(total_r / total, 4) if total else 0.0,
        expectancy_r=round(total_r / total, 4) if total else 0.0,
        total_r=round(total_r, 4),
        by_symbol=breakdown("symbol"),
        by_timeframe=breakdown("timeframe"),
        by_signal_type=breakdown("signal_type"),
        by_outcome=breakdown("outcome"),
    )


async def refresh_signal_outcomes(signals: Iterable[Signal], *, store: SignalOutcomeStore | None = None) -> list[SignalOutcome]:
    storage = store or SignalOutcomeStore()
    signal_list = list(signals)
    if not signal_list:
        return []

    max_bars = int(os.getenv("DASHBOARD_SIGNAL_LOOKAHEAD_BARS", "120"))
    base_url = os.getenv("BYBIT_PUBLIC_BASE_URL", DEFAULT_BYBIT_BASE_URL)

    from orderflow_accum.bybit_rest import BybitRestClient

    outcomes: list[SignalOutcome] = []
    async with BybitRestClient(base_url=base_url, timeout_seconds=25, retries=2) as client:
        for signal in signal_list:
            interval = normalize_interval(signal.timeframe)
            start_ms = int(signal.created_at.timestamp() * 1000)
            end_ms = start_ms + interval_milliseconds(signal.timeframe) * max_bars
            df = await client.fetch_klines(signal.symbol, interval, limit=max_bars, start=start_ms, end=end_ms)
            records = df.to_dict("records") if hasattr(df, "to_dict") else []
            outcomes.append(calculate_signal_outcome(signal, records, max_bars=max_bars))
            await asyncio.sleep(0)

    storage.upsert_many(outcomes)
    return outcomes
