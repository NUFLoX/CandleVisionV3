from __future__ import annotations

import sqlite3

from .research_runs import ResearchRunLedger


_ENTRY_ACTIONS = frozenset({"ENTER_LONG", "ENTER_SHORT"})


def _text(value: object | None) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def is_research_entry_action(action: object | None) -> bool:
    normalized = _text(action)

    if normalized is None:
        return False

    return normalized.upper() in _ENTRY_ACTIONS


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_run_signal_membership (
            run_id TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            entered_at TEXT NOT NULL,
            PRIMARY KEY (run_id, signal_key),
            FOREIGN KEY (run_id) REFERENCES research_runs(run_id)
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_research_membership_run
        ON research_run_signal_membership(run_id, entered_at DESC)
        """
    )

    conn.commit()


def enroll_research_signal(
    ledger: ResearchRunLedger,
    *,
    signal_key: str,
    entered_at: object | None,
) -> bool:
    if not ledger.run_id:
        return False

    normalized_key = _text(signal_key)
    normalized_time = _text(entered_at)

    if not normalized_key or not normalized_time:
        return False

    _ensure_schema(ledger.conn)

    ledger.conn.execute(
        """
        INSERT OR IGNORE INTO research_run_signal_membership (
            run_id,
            signal_key,
            entered_at
        )
        VALUES (?, ?, ?)
        """,
        (
            ledger.run_id,
            normalized_key,
            normalized_time,
        ),
    )

    ledger.conn.commit()
    return True


def has_research_signal_membership(
    ledger: ResearchRunLedger,
    *,
    signal_key: str,
) -> bool:
    if not ledger.run_id:
        return False

    normalized_key = _text(signal_key)

    if not normalized_key:
        return False

    _ensure_schema(ledger.conn)

    row = ledger.conn.execute(
        """
        SELECT 1
        FROM research_run_signal_membership
        WHERE run_id = ?
          AND signal_key = ?
        LIMIT 1
        """,
        (
            ledger.run_id,
            normalized_key,
        ),
    ).fetchone()

    return row is not None
