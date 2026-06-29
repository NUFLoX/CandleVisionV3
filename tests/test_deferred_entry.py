from __future__ import annotations

from datetime import datetime, timedelta, timezone

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_CANCELLED,
    DEFERRED_ENTRY_INVALIDATED,
    DEFERRED_ENTRY_PENDING,
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
    DeferredEntryCandidate,
    DeferredEntrySnapshot,
    DeferredEntryState,
    DeferredEntryStore,
    evaluate_deferred_entry,
)


NOW = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)


def _candidate(
    *,
    reason: str = "entry_blocked_buy_flow",
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
        expires_at=NOW + timedelta(hours=24),
        origin_support=94.0,
        origin_ema20=96.0,
        origin_vwap=96.5,
        metadata={"source": "unit_test"},
    )


def _snapshot(
    price: float,
    *,
    buy_flow: float = 100.0,
    sell_flow: float = 100.0,
    volume_impulse: float = 0.50,
    ask_wall_strength: float = 0.30,
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


def _state_from(result) -> DeferredEntryState:
    return DeferredEntryState(
        status=result.status,
        lowest_price=result.lowest_price,
        highest_price=result.highest_price,
        pullback_seen=result.pullback_seen,
    )


def test_waits_for_pullback_then_confirms_reclaim():
    candidate = _candidate()

    first = evaluate_deferred_entry(
        candidate,
        DeferredEntryState(),
        _snapshot(100.0),
        now=NOW,
    )

    assert first.status == DEFERRED_ENTRY_PENDING
    assert first.reason == (
        "deferred_entry_waiting_for_controlled_pullback"
    )
    assert not first.allowed_to_enter

    second = evaluate_deferred_entry(
        candidate,
        _state_from(first),
        _snapshot(95.0),
        now=NOW + timedelta(hours=1),
    )

    assert second.status == DEFERRED_ENTRY_PULLBACK_SEEN
    assert second.pullback_seen
    assert second.pullback_r == 0.5

    third = evaluate_deferred_entry(
        candidate,
        _state_from(second),
        _snapshot(
            97.0,
            buy_flow=120.0,
            sell_flow=100.0,
            volume_impulse=0.80,
            ask_wall_strength=0.20,
        ),
        now=NOW + timedelta(hours=2),
    )

    assert third.status == DEFERRED_ENTRY_READY
    assert third.reason == (
        "deferred_entry_reclaim_confirmed"
    )
    assert third.allowed_to_enter
    assert third.reclaim_level == 96.5


def test_invalidates_when_original_structural_stop_breaks():
    result = evaluate_deferred_entry(
        _candidate(),
        DeferredEntryState(),
        _snapshot(90.0),
        now=NOW,
    )

    assert result.status == DEFERRED_ENTRY_INVALIDATED
    assert result.reason == (
        "deferred_entry_structural_stop_invalidated"
    )
    assert not result.allowed_to_enter


def test_cancels_when_reclaim_is_already_too_late():
    candidate = _candidate()
    state = DeferredEntryState(
        status=DEFERRED_ENTRY_PULLBACK_SEEN,
        lowest_price=95.0,
        highest_price=100.0,
        pullback_seen=True,
    )

    result = evaluate_deferred_entry(
        candidate,
        state,
        _snapshot(
            103.0,
            buy_flow=120.0,
            sell_flow=100.0,
            volume_impulse=0.80,
        ),
        now=NOW + timedelta(hours=2),
    )

    assert result.status == DEFERRED_ENTRY_CANCELLED
    assert result.reason == (
        "deferred_entry_reclaim_too_late"
    )


def test_non_transient_block_reason_is_not_deferred():
    result = evaluate_deferred_entry(
        _candidate(reason="entry_blocked_bad_rr"),
        DeferredEntryState(),
        _snapshot(95.0),
        now=NOW,
    )

    assert result.status == DEFERRED_ENTRY_CANCELLED
    assert result.reason == (
        "deferred_entry_initial_block_not_eligible"
    )


def test_store_persists_candidate_and_evaluation(tmp_path):
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )

    try:
        candidate = _candidate()
        created = store.create_or_get(candidate)

        assert created["status"] == DEFERRED_ENTRY_PENDING
        assert created["metadata_json"]["source"] == "unit_test"

        result = evaluate_deferred_entry(
            candidate,
            DeferredEntryState(),
            _snapshot(95.0),
            now=NOW + timedelta(hours=1),
        )
        updated = store.apply_evaluation(
            candidate.signal_key,
            result,
            _snapshot(95.0),
        )

        assert updated["status"] == DEFERRED_ENTRY_PULLBACK_SEEN
        assert updated["pullback_seen"] == 1
        assert updated["lowest_price"] == 95.0
        assert len(store.list_active()) == 1
    finally:
        store.close()
