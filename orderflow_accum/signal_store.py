from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _phase_from_kind(kind: str) -> str:
    k = (kind or "").upper()

    if k == "ACCUMULATION_WATCH":
        return "WATCHING"

    if k == "ABSORPTION_ZONE":
        return "ACCUMULATION"

    if k == "PRE_IMPULSE_ZONE":
        return "PRE_IMPULSE"

    if k == "BREAKOUT_PRESSURE":
        return "BREAKOUT_PRESSURE"

    return "PENDING"


@dataclass(slots=True)
class UpsertResult:
    is_new: bool
    should_notify: bool
    status_changed: bool
    score_jump: bool
    from_status: str | None
    to_status: str
    repeat_count: int


class SignalStore:
    SCHEMA_VERSION = 2

    def __init__(
        self,
        db_path: str = "data/signals.db",
        score_jump_threshold: float = 2.0,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.score_jump_threshold = score_jump_threshold
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cur = self.conn.cursor()

        cur.execute("PRAGMA user_version")
        user_version = int(cur.fetchone()[0])

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                side TEXT NOT NULL,
                score_first REAL NOT NULL,
                score_last REAL NOT NULL,
                score_max REAL NOT NULL,
                entry REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit_1 REAL NOT NULL,
                take_profit_2 REAL NOT NULL,
                reasons_first TEXT NOT NULL,
                reasons_last TEXT NOT NULL,
                meta TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                repeat_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                outcome TEXT,
                outcome_checked_at TEXT,
                time_to_tp1_minutes REAL,
                time_to_tp2_minutes REAL,
                time_to_sl_minutes REAL,
                max_gain_pct REAL,
                max_drawdown_pct REAL
            )
            """
        )

        for col, typ in (
            ("outcome", "TEXT"),
            ("outcome_checked_at", "TEXT"),
            ("time_to_tp1_minutes", "REAL"),
            ("time_to_tp2_minutes", "REAL"),
            ("time_to_sl_minutes", "REAL"),
            ("max_gain_pct", "REAL"),
            ("max_drawdown_pct", "REAL"),
        ):
            try:
                cur.execute(f"ALTER TABLE signals ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_symbol_tf
            ON signals(symbol, timeframe)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_status
            ON signals(status)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                event_type TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT,
                score_last REAL,
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signal_events_key
            ON signal_events(signal_key, created_at)
            """
        )

        if user_version < self.SCHEMA_VERSION:
            cur.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")

        self.conn.commit()

    def add_event(
        self,
        *,
        signal_key: str,
        symbol: str,
        timeframe: str,
        event_type: str,
        from_status: str | None,
        to_status: str | None,
        score_last: float | None = None,
    ) -> None:
        cur = self.conn.cursor()

        cur.execute(
            """
            INSERT INTO signal_events (
                signal_key,
                symbol,
                timeframe,
                event_type,
                from_status,
                to_status,
                score_last,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_key,
                symbol,
                timeframe,
                event_type,
                from_status,
                to_status,
                score_last,
                _utc_now(),
            ),
        )

        self.conn.commit()

    def upsert_signal(self, signal: Any, *, market: str = "linear") -> UpsertResult:
        symbol = str(getattr(signal, "symbol", "UNKNOWN"))
        side = str(getattr(signal, "side", "Buy"))
        kind = str(getattr(signal, "kind", "SIGNAL"))
        source = str(getattr(signal, "source", "orderflow"))

        score = float(getattr(signal, "score", 0.0) or 0.0)
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        sl = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        tp1 = float(getattr(signal, "take_profit_1", 0.0) or 0.0)
        tp2 = float(getattr(signal, "take_profit_2", 0.0) or 0.0)

        reasons = list(getattr(signal, "reasons", []) or [])
        meta = dict(getattr(signal, "meta", {}) or {})

        timeframe = str(meta.get("tf") or "1")
        status = _phase_from_kind(kind)
        signal_key = f"{symbol}|{market}|{timeframe}|{kind}|{side}"
        now = _utc_now()

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM signals WHERE signal_key = ?", (signal_key,))
        row = cur.fetchone()

        if row is None:
            cur.execute(
                """
                INSERT INTO signals (
                    signal_key,
                    symbol,
                    market,
                    timeframe,
                    source,
                    kind,
                    side,
                    score_first,
                    score_last,
                    score_max,
                    entry,
                    stop_loss,
                    take_profit_1,
                    take_profit_2,
                    reasons_first,
                    reasons_last,
                    meta,
                    first_seen,
                    last_seen,
                    repeat_count,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_key,
                    symbol,
                    market,
                    timeframe,
                    source,
                    kind,
                    side,
                    score,
                    score,
                    score,
                    entry,
                    sl,
                    tp1,
                    tp2,
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(meta, ensure_ascii=False),
                    now,
                    now,
                    1,
                    status,
                ),
            )

            self.conn.commit()

            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="new_setup",
                from_status=None,
                to_status=status,
                score_last=score,
            )

            return UpsertResult(
                is_new=True,
                should_notify=True,
                status_changed=False,
                score_jump=False,
                from_status=None,
                to_status=status,
                repeat_count=1,
            )

        prev_status = str(row["status"])
        prev_score = float(row["score_last"])
        prev_max = float(row["score_max"])
        repeat_count = int(row["repeat_count"]) + 1

        score_jump = (score - prev_score) >= self.score_jump_threshold
        status_changed = status != prev_status
        should_notify = status_changed or score_jump

        cur.execute(
            """
            UPDATE signals
            SET
                score_last = ?,
                score_max = ?,
                reasons_last = ?,
                meta = ?,
                last_seen = ?,
                repeat_count = ?,
                status = ?,
                entry = ?,
                stop_loss = ?,
                take_profit_1 = ?,
                take_profit_2 = ?
            WHERE signal_key = ?
            """,
            (
                score,
                max(prev_max, score),
                json.dumps(reasons, ensure_ascii=False),
                json.dumps(meta, ensure_ascii=False),
                now,
                repeat_count,
                status,
                entry,
                sl,
                tp1,
                tp2,
                signal_key,
            ),
        )

        self.conn.commit()

        if status_changed:
            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="status_changed",
                from_status=prev_status,
                to_status=status,
                score_last=score,
            )

        if score_jump:
            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="score_jump",
                from_status=prev_status,
                to_status=status,
                score_last=score,
            )

        if not status_changed and not score_jump:
            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="repeat",
                from_status=prev_status,
                to_status=status,
                score_last=score,
            )

        return UpsertResult(
            is_new=False,
            should_notify=should_notify,
            status_changed=status_changed,
            score_jump=score_jump,
            from_status=prev_status,
            to_status=status,
            repeat_count=repeat_count,
        )

    def close(self) -> None:
        self.conn.close()
