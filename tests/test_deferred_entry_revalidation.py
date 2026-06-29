from __future__ import annotations

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
)
from orderflow_accum.deferred_entry_revalidation import (
    revalidate_ready_deferred_entry,
)
from orderflow_accum.trade_executor import (
    ENTER_LONG,
    WATCH,
    OrderflowSnapshot,
    TradeDecision,
    TradeSetup,
)


KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"


class FakeStrictExecutor:
    def __init__(self, decision: TradeDecision) -> None:
        self.decision = decision
        self.calls = 0

    def evaluate_entry(
        self,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
    ) -> TradeDecision:
        del setup, snapshot
        self.calls += 1
        return self.decision


def _record(*, status: str = DEFERRED_ENTRY_READY) -> dict[str, object]:
    return {
        "signal_key": KEY,
        "status": status,
        "side": "Buy",
        "timeframe": "60",
    }


def _setup() -> TradeSetup:
    return TradeSetup(
        symbol="TESTUSDT",
        side="Buy",
        entry_hint=100.0,
        stop_loss=90.0,
        score=12.0,
        timeframe="60",
        btc_regime="BTC_NEUTRAL",
        market_regime="BTC_NEUTRAL",
        reasons=["PRE_IMPULSE_ZONE"],
        signal_kind="PRE_IMPULSE_ZONE",
    )


def _snapshot() -> OrderflowSnapshot:
    return OrderflowSnapshot(
        price=97.0,
        spread_bps=2.0,
        buy_flow=140.0,
        sell_flow=100.0,
        bid_wall_strength=0.10,
        ask_wall_strength=0.20,
        volume_impulse=1.30,
        support=96.0,
        resistance=110.0,
        ema20=96.5,
        vwap=96.0,
        candle_close=97.0,
    )


def _allowed_decision() -> TradeDecision:
    return TradeDecision(
        ENTER_LONG,
        "entry_allowed_long",
        "ENTERED",
        None,
    )


def _watch_decision(reason: str) -> TradeDecision:
    return TradeDecision(
        WATCH,
        reason,
        "TRADE_WATCH",
        None,
    )


def _revalidate(
    executor: FakeStrictExecutor,
    *,
    record: dict[str, object] | None = None,
    snapshot_fresh: bool = True,
    h4_allowed: bool = True,
    h4_reason: str | None = "h4_not_confirmed_bearish",
    guard_decisions=(),
):
    return revalidate_ready_deferred_entry(
        record=record or _record(),
        mode="paper",
        setup=_setup(),
        snapshot=_snapshot(),
        snapshot_fresh=snapshot_fresh,
        h4_allowed=h4_allowed,
        h4_reason=h4_reason,
        executor=executor,
        guard_decisions=guard_decisions,
    )


def test_ready_revalidation_calls_strict_executor_without_opening():
    executor = FakeStrictExecutor(_allowed_decision())

    result = _revalidate(executor)

    assert result.allowed_to_enter is True
    assert result.reason == (
        "deferred_entry_revalidation_strict_gates_passed"
    )
    assert executor.calls == 1
    assert result.executor_decision is executor.decision
    assert result.diagnostics[
        "deferred_entry_revalidation_allowed"
    ] is True


def test_revalidation_rejects_non_ready_before_executor_call():
    executor = FakeStrictExecutor(_allowed_decision())

    result = _revalidate(
        executor,
        record=_record(status=DEFERRED_ENTRY_PULLBACK_SEEN),
    )

    assert result.allowed_to_enter is False
    assert result.reason == "deferred_entry_revalidation_not_ready"
    assert result.executor_decision is None
    assert executor.calls == 0


def test_revalidation_rejects_stale_snapshot_before_executor_call():
    executor = FakeStrictExecutor(_allowed_decision())

    result = _revalidate(
        executor,
        snapshot_fresh=False,
    )

    assert result.allowed_to_enter is False
    assert result.reason == (
        "deferred_entry_revalidation_missing_fresh_orderflow"
    )
    assert result.executor_decision is None
    assert executor.calls == 0


def test_revalidation_rejects_h4_before_executor_call():
    executor = FakeStrictExecutor(_allowed_decision())

    result = _revalidate(
        executor,
        h4_allowed=False,
        h4_reason="entry_blocked_h4_bearish_structure",
    )

    assert result.allowed_to_enter is False
    assert result.reason == "entry_blocked_h4_bearish_structure"
    assert result.executor_decision is None
    assert executor.calls == 0


def test_revalidation_keeps_strict_executor_watch_reason():
    executor = FakeStrictExecutor(
        _watch_decision("entry_blocked_buy_flow")
    )

    result = _revalidate(executor)

    assert result.allowed_to_enter is False
    assert result.reason == "entry_blocked_buy_flow"
    assert result.executor_decision is executor.decision
    assert executor.calls == 1


def test_revalidation_rejects_final_guard_after_executor_allows():
    executor = FakeStrictExecutor(_allowed_decision())
    target_guard = _watch_decision(
        "entry_blocked_target_quality_rr_tp1"
    )

    result = _revalidate(
        executor,
        guard_decisions=[
            ("target_quality", target_guard),
            ("rr", None),
            ("stop_loss", None),
        ],
    )

    assert result.allowed_to_enter is False
    assert result.reason == (
        "entry_blocked_target_quality_rr_tp1"
    )
    assert result.executor_decision is executor.decision
    assert executor.calls == 1
    assert result.diagnostics[
        "deferred_entry_revalidation_blocking_guard"
    ] == "target_quality"
