from __future__ import annotations

from datetime import datetime, timedelta, timezone

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_EXPIRED,
    DEFERRED_ENTRY_INVALIDATED,
    DEFERRED_ENTRY_PENDING,
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
    DeferredEntryCandidate,
    DeferredEntrySnapshot,
    DeferredEntryStore,
)
from orderflow_accum.deferred_entry_refresh_service import (
    DeferredEntryRefreshService,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)


NOW = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)


def _candidate(
    signal_key: str,
    *,
    expires_at: datetime | None = None,
) -> DeferredEntryCandidate:
    return DeferredEntryCandidate(
        signal_key=signal_key,
        symbol="TESTUSDT",
        market="linear",
        timeframe="60",
        side="Buy",
        signal_kind="PRE_IMPULSE_ZONE",
        origin_entry=100.0,
        origin_stop_loss=90.0,
        score=12.0,
        initial_block_reason="entry_blocked_buy_flow",
        created_at=NOW,
        expires_at=expires_at or (
            NOW + timedelta(hours=24)
        ),
        origin_support=94.0,
        origin_ema20=96.0,
        origin_vwap=96.5,
        metadata={
            "source": "unit_test",
            "confirmed_status": "PRE_IMPULSE",
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


def _service(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)
    service = DeferredEntryRefreshService(
        coordinator,
        max_active=12,
    )
    return service, coordinator, store


def test_refresh_batch_advances_pullback_then_ready(
    tmp_path,
):
    service, coordinator, store = _service(tmp_path)
    key = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"

    try:
        registration = coordinator.register(_candidate(key))
        assert registration is not None

        pullback = service.refresh_active(
            {
                key: _snapshot(95.0),
            },
            now=NOW + timedelta(hours=1),
        )

        assert pullback.attempted == 1
        assert pullback.refreshed == 1
        assert pullback.ready_signal_keys == ()

        row = store.get(key)
        assert row is not None
        assert row["status"] == (
            DEFERRED_ENTRY_PULLBACK_SEEN
        )

        reclaim = service.refresh_active(
            {
                key: _snapshot(
                    97.0,
                    buy_flow=120.0,
                    sell_flow=100.0,
                    volume_impulse=0.80,
                ),
            },
            now=NOW + timedelta(hours=2),
        )

        assert reclaim.refreshed == 1
        assert reclaim.ready_signal_keys == (key,)

        row = store.get(key)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_READY
        assert row["last_reason"] == (
            "deferred_entry_reclaim_confirmed"
        )
    finally:
        store.close()


def test_refresh_batch_skips_candidate_without_snapshot(
    tmp_path,
):
    service, coordinator, store = _service(tmp_path)
    key = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"

    try:
        registration = coordinator.register(_candidate(key))
        assert registration is not None

        result = service.refresh_active(
            {},
            now=NOW + timedelta(hours=1),
        )

        assert result.attempted == 1
        assert result.refreshed == 0
        assert result.skipped_missing_snapshot_keys == (
            key,
        )

        row = store.get(key)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_PENDING
        assert row["last_reason"] is None
    finally:
        store.close()


def test_refresh_batch_persists_invalidated_and_expired(
    tmp_path,
):
    service, coordinator, store = _service(tmp_path)

    invalid_key = (
        "INVALIDUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"
    )
    expired_key = (
        "EXPIREDUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"
    )

    try:
        invalid = coordinator.register(
            _candidate(invalid_key)
        )
        expired = coordinator.register(
            _candidate(
                expired_key,
                expires_at=NOW,
            )
        )

        assert invalid is not None
        assert expired is not None

        result = service.refresh_active(
            {
                invalid_key: _snapshot(90.0),
                expired_key: _snapshot(95.0),
            },
            now=NOW + timedelta(seconds=1),
        )

        assert result.refreshed == 2
        assert set(result.terminal_signal_keys) == {
            invalid_key,
            expired_key,
        }

        invalid_row = store.get(invalid_key)
        expired_row = store.get(expired_key)

        assert invalid_row is not None
        assert expired_row is not None
        assert invalid_row["status"] == (
            DEFERRED_ENTRY_INVALIDATED
        )
        assert expired_row["status"] == (
            DEFERRED_ENTRY_EXPIRED
        )
    finally:
        store.close()


def test_refresh_service_has_no_order_execution_surface(
    tmp_path,
):
    service, coordinator, store = _service(tmp_path)

    try:
        assert not hasattr(service, "open_position")
        assert not hasattr(service, "execute_order")
        assert not hasattr(service, "refresh_open_executor_positions")
    finally:
        store.close()
