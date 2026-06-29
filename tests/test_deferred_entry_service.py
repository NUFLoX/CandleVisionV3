from __future__ import annotations

from datetime import datetime, timedelta, timezone

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_EXPIRED,
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
    DeferredEntryCandidate,
    DeferredEntrySnapshot,
    DeferredEntryStore,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)


NOW = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)


def _candidate(
    *,
    reason: str = "entry_blocked_buy_flow",
    expires_at: datetime | None = None,
) -> DeferredEntryCandidate:
    return DeferredEntryCandidate(
        signal_key="TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy",
        symbol="TESTUSDT",
        market="linear",
        timeframe="60",
        side="Buy",
        signal_kind="PRE_IMPULSE_ZONE",
        origin_entry=100.0,
        origin_stop_loss=90.0,
        score=12.0,
        initial_block_reason=reason,
        created_at=NOW,
        expires_at=expires_at or (
            NOW + timedelta(hours=24)
        ),
        origin_support=94.0,
        origin_ema20=96.0,
        origin_vwap=96.5,
        metadata={
            "confirmed_status": "PRE_IMPULSE",
            "source": "unit_test",
        },
    )


def _snapshot(
    price: float,
    *,
    buy_flow: float = 100.0,
    sell_flow: float = 100.0,
    volume_impulse: float = 0.50,
    ask_wall_strength: float = 0.20,
) -> DeferredEntrySnapshot:
    return DeferredEntrySnapshot(
        price=price,
        buy_flow=buy_flow,
        sell_flow=sell_flow,
        volume_impulse=volume_impulse,
        ask_wall_strength=ask_wall_strength,
        support=94.0,
        ema20=96.0,
        vwap=96.5,
        candle_close=price,
    )


def test_register_persists_eligible_candidate(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    try:
        registration = coordinator.register(_candidate())

        assert registration is not None
        assert registration.created is True
        assert registration.record["signal_key"] == (
            "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"
        )
        assert registration.record["metadata_json"][
            "source"
        ] == "unit_test"
    finally:
        store.close()


def test_register_returns_existing_record_without_duplicate(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    try:
        first = coordinator.register(_candidate())
        second = coordinator.register(_candidate())

        assert first is not None
        assert second is not None
        assert first.created is True
        assert second.created is False
        assert len(store.list_active()) == 1
    finally:
        store.close()


def test_register_rejects_structural_block_reason(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    try:
        registration = coordinator.register(
            _candidate(reason="entry_blocked_bad_rr")
        )

        assert registration is None
        assert store.list_active() == []
    finally:
        store.close()


def test_refresh_persists_pullback_then_reclaim(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    try:
        registration = coordinator.register(_candidate())
        assert registration is not None

        pullback = coordinator.refresh(
            registration.record,
            _snapshot(95.0),
            now=NOW + timedelta(hours=1),
        )

        assert pullback.evaluation.status == (
            DEFERRED_ENTRY_PULLBACK_SEEN
        )
        assert pullback.record["pullback_seen"] == 1

        reclaim = coordinator.refresh(
            pullback.record,
            _snapshot(
                97.0,
                buy_flow=120.0,
                sell_flow=100.0,
                volume_impulse=0.80,
            ),
            now=NOW + timedelta(hours=2),
        )

        assert reclaim.evaluation.status == (
            DEFERRED_ENTRY_READY
        )
        assert reclaim.evaluation.allowed_to_enter is True
        assert reclaim.state_changed is True
    finally:
        store.close()


def test_refresh_marks_expired_candidate(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    try:
        registration = coordinator.register(
            _candidate(expires_at=NOW)
        )
        assert registration is not None

        result = coordinator.refresh(
            registration.record,
            _snapshot(100.0),
            now=NOW + timedelta(seconds=1),
        )

        assert result.evaluation.status == (
            DEFERRED_ENTRY_EXPIRED
        )
        assert result.record["status"] == (
            DEFERRED_ENTRY_EXPIRED
        )
    finally:
        store.close()
