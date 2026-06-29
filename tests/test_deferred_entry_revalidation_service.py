from __future__ import annotations

import sqlite3

from datetime import datetime, timedelta, timezone

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
    DeferredEntryCandidate,
    DeferredEntryStore,
)
from orderflow_accum.deferred_entry_revalidation import (
    DeferredEntryRevalidationResult,
)
from orderflow_accum.deferred_entry_revalidation_service import (
    DeferredEntryRevalidationService,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)


KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"


def _candidate() -> DeferredEntryCandidate:
    now = datetime.now(timezone.utc)

    return DeferredEntryCandidate(
        signal_key=KEY,
        symbol="TESTUSDT",
        market="linear",
        timeframe="60",
        side="Buy",
        signal_kind="PRE_IMPULSE_ZONE",
        origin_entry=100.0,
        origin_stop_loss=90.0,
        score=12.0,
        initial_block_reason="entry_blocked_buy_flow",
        created_at=now,
        expires_at=now + timedelta(hours=24),
    )


def _result(
    *,
    signal_key: str = KEY,
    allowed: bool = True,
    reason: str = (
        "deferred_entry_revalidation_strict_gates_passed"
    ),
) -> DeferredEntryRevalidationResult:
    return DeferredEntryRevalidationResult(
        signal_key=signal_key,
        allowed_to_enter=allowed,
        reason=reason,
        executor_decision=None,
        diagnostics={
            "deferred_entry_revalidation_allowed": allowed,
            "deferred_entry_revalidation_price": 97.0,
        },
    )


def _ready_store(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)
    coordinator.register(_candidate())

    store.conn.execute(
        """
        UPDATE deferred_entries
        SET status = ?
        WHERE signal_key = ?
        """,
        (DEFERRED_ENTRY_READY, KEY),
    )
    store.conn.commit()

    return store, coordinator


def test_persist_ready_result_keeps_ready_status(tmp_path):
    store, coordinator = _ready_store(tmp_path)

    try:
        service = DeferredEntryRevalidationService(coordinator)

        batch = service.persist_ready_results([
            _result(),
        ])

        assert batch.attempted == 1
        assert batch.persisted == 1
        assert not batch.skipped_missing
        assert not batch.skipped_not_ready

        row = store.get(KEY)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_READY
        assert row["last_reason"] == (
            "deferred_entry_revalidation_strict_gates_passed"
        )
        assert row["revalidation_json"][
            "attempt_count"
        ] == 1
        assert row["revalidation_json"][
            "allowed_to_enter"
        ] is True
        assert row["revalidation_json"][
            "diagnostics"
        ]["deferred_entry_revalidation_price"] == 97.0
    finally:
        store.close()


def test_revalidation_attempt_counter_increments(tmp_path):
    store, coordinator = _ready_store(tmp_path)

    try:
        service = DeferredEntryRevalidationService(coordinator)

        first = service.persist_ready_results([
            _result(),
        ])
        second = service.persist_ready_results([
            _result(
                allowed=False,
                reason="entry_blocked_h4_bearish_structure",
            ),
        ])

        assert first.persisted == 1
        assert second.persisted == 1

        row = store.get(KEY)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_READY
        assert row["revalidation_json"][
            "attempt_count"
        ] == 2
        assert row["revalidation_json"][
            "allowed_to_enter"
        ] is False
        assert row["revalidation_json"][
            "reason"
        ] == "entry_blocked_h4_bearish_structure"
    finally:
        store.close()


def test_service_skips_non_ready_and_missing_records(tmp_path):
    store, coordinator = _ready_store(tmp_path)

    try:
        store.conn.execute(
            """
            UPDATE deferred_entries
            SET status = ?
            WHERE signal_key = ?
            """,
            (DEFERRED_ENTRY_PULLBACK_SEEN, KEY),
        )
        store.conn.commit()

        service = DeferredEntryRevalidationService(coordinator)

        batch = service.persist_ready_results([
            _result(),
            _result(signal_key="MISSING|linear|60|X|Buy"),
        ])

        assert batch.attempted == 2
        assert batch.persisted == 0
        assert batch.skipped_not_ready == (KEY,)
        assert batch.skipped_missing == (
            "MISSING|linear|60|X|Buy",
        )

        row = store.get(KEY)
        assert row is not None
        assert row["status"] == (
            DEFERRED_ENTRY_PULLBACK_SEEN
        )
        assert row["revalidation_json"] == {}
    finally:
        store.close()

def test_legacy_schema_is_migrated_for_revalidation_json(
    tmp_path,
):
    db_path = tmp_path / "legacy-signals.db"

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE deferred_entries (
                signal_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                side TEXT NOT NULL,
                signal_kind TEXT NOT NULL,
                origin_entry REAL NOT NULL,
                origin_stop_loss REAL NOT NULL,
                origin_score REAL NOT NULL,
                initial_block_reason TEXT NOT NULL,
                origin_support REAL,
                origin_ema20 REAL,
                origin_vwap REAL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                lowest_price REAL,
                highest_price REAL,
                pullback_seen INTEGER NOT NULL DEFAULT 0,
                last_reason TEXT,
                last_reclaim_level REAL,
                ready_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                last_snapshot_json TEXT NOT NULL DEFAULT '{}',
                diagnostics_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    store = DeferredEntryStore(str(db_path))
    try:
        columns = {
            str(row[1])
            for row in store.conn.execute(
                "PRAGMA table_info(deferred_entries)"
            ).fetchall()
        }

        assert "revalidation_json" in columns
    finally:
        store.close()
