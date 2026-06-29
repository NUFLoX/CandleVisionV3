from __future__ import annotations

from types import SimpleNamespace

from orderflow_accum.deferred_entry import DeferredEntryStore
from orderflow_accum.deferred_entry_runtime import (
    DeferredEntryRuntime,
    DeferredEntryRuntimeConfig,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)
from orderflow_accum.runner import AccumulationRunner
from orderflow_accum.trade_executor import (
    WATCH,
    OrderflowSnapshot,
    SmartTradeExecutor,
    TradeSetup,
)


KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"


class FakeSignalStore:
    def get_executor_outcome(self, signal_key: str):
        del signal_key
        return None


def _signal():
    return SimpleNamespace(
        symbol="TESTUSDT",
        side="Buy",
        kind="PRE_IMPULSE_ZONE",
        source="orderflow",
        score=12.0,
        entry=100.0,
        stop_loss=90.0,
        take_profit_1=115.0,
        take_profit_2=130.0,
        reasons=["PRE_IMPULSE_ZONE"],
        meta={
            "market": "linear",
            "tf": "60",
            "btc_regime": "BTC_NEUTRAL",
            "market_regime": "RISK_ON",
        },
    )


def _setup():
    return TradeSetup(
        symbol="TESTUSDT",
        side="Buy",
        entry_hint=100.0,
        stop_loss=90.0,
        score=12.0,
        timeframe="60",
        btc_regime="BTC_NEUTRAL",
        market_regime="RISK_ON",
        signal_kind="PRE_IMPULSE_ZONE",
        reasons=["PRE_IMPULSE_ZONE"],
    )


def _snapshot(
    *,
    support: float = 94.0,
    buy_flow: float = 100.0,
    sell_flow: float = 100.0,
    volume_impulse: float = 1.20,
):
    return OrderflowSnapshot(
        price=100.0,
        spread_bps=4.0,
        buy_flow=buy_flow,
        sell_flow=sell_flow,
        bid_wall_strength=0.10,
        ask_wall_strength=0.20,
        volume_impulse=volume_impulse,
        support=support,
        resistance=115.0,
        ema20=96.0,
        vwap=96.5,
        candle_close=100.0,
    )


def _runner(
    tmp_path,
    *,
    should_process: bool = True,
):
    runner = AccumulationRunner.__new__(AccumulationRunner)

    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )

    runner.signal_store = FakeSignalStore()
    runner.deferred_entry_store = store
    runner.deferred_entry_runtime = DeferredEntryRuntime(
        DeferredEntryCoordinator(store),
        config=DeferredEntryRuntimeConfig(
            enabled=True,
            ttl_hours=24.0,
            h1_only=True,
        ),
    )

    runner.trade_executor_enabled = True
    runner.trade_executor_mode = "paper"
    runner.trade_executor = SmartTradeExecutor()

    runner._should_process_paper_executor_status = (
        lambda signal, status: (should_process, {})
    )
    runner._signal_key = lambda signal, market: KEY
    runner._paper_executor_setup = lambda signal: _setup()
    runner._missed_signal_memory_diagnostics = lambda setup: {}
    runner._observe_hybrid_entry_shadow = (
        lambda *args, **kwargs: None
    )
    runner._is_terminal_executor_outcome = (
        lambda row: False
    )
    runner._executor_buy_momentum_override_decision = (
        lambda setup, snapshot, decision: decision
    )
    runner._executor_side_allowed = lambda side: True
    runner._evaluate_stop_reclaim_reentry = (
        lambda *args, **kwargs: (None, {})
    )
    runner._evaluate_early_breakout_entry = (
        lambda *args, **kwargs: (None, {})
    )
    runner._executor_target_quality_gate = (
        lambda *args, **kwargs: (None, {})
    )
    runner._entry_risk_reward_guard = (
        lambda *args, **kwargs: (None, {})
    )
    runner._entry_stop_loss_guard = (
        lambda *args, **kwargs: (None, {})
    )
    runner._executor_symbol_blocked = lambda symbol: False

    captured = []

    def capture_store(
        signal_key,
        signal,
        decision,
        position=None,
        snapshot=None,
        **kwargs,
    ):
        captured.append(
            {
                "signal_key": signal_key,
                "decision": decision,
                "position": position,
                "snapshot": snapshot,
                "context": dict(
                    kwargs.get("observation_context") or {}
                ),
            }
        )
        return {}

    runner._store_paper_executor_decision = capture_store

    return runner, store, captured


def test_runner_registers_transient_final_watch(
    tmp_path,
):
    runner, store, captured = _runner(tmp_path)

    try:
        runner._paper_executor_snapshot = (
            lambda signal, state=None: (
                _snapshot(),
                False,
            )
        )

        runner._process_paper_executor(
            _signal(),
            "linear",
            "PRE_IMPULSE",
            h4_entry_context={
                "h4_entry_gate_allowed": True,
            },
        )

        row = store.get(KEY)

        assert row is not None
        assert row["initial_block_reason"] == (
            "entry_blocked_buy_flow"
        )

        assert len(captured) == 1
        assert captured[0]["decision"].action == WATCH
        assert captured[0]["decision"].reason == (
            "entry_blocked_buy_flow"
        )
        assert captured[0]["context"][
            "deferred_entry_registered"
        ] is True
        assert captured[0]["context"][
            "deferred_entry_registration_reason"
        ] == "deferred_entry_created"
    finally:
        store.close()


def test_runner_rejects_watch_with_structural_blocker(
    tmp_path,
):
    runner, store, captured = _runner(tmp_path)

    try:
        runner._paper_executor_snapshot = (
            lambda signal, state=None: (
                _snapshot(support=101.0),
                False,
            )
        )

        runner._process_paper_executor(
            _signal(),
            "linear",
            "PRE_IMPULSE",
            h4_entry_context={
                "h4_entry_gate_allowed": True,
            },
        )

        assert store.get(KEY) is None
        assert len(captured) == 1
        assert captured[0]["decision"].action == WATCH
        assert captured[0]["decision"].reason == (
            "entry_blocked_buy_flow"
        )
        assert captured[0]["context"][
            "deferred_entry_registered"
        ] is False
        assert captured[0]["context"][
            "deferred_entry_registration_reason"
        ] == "deferred_entry_structural_gate_not_allowed"
        assert "entry_blocked_below_support" in captured[
            0
        ]["context"]["deferred_entry_structural_blockers"]
    finally:
        store.close()


def test_runner_deferred_probe_registers_early_watch(
    tmp_path,
):
    runner, store, captured = _runner(
        tmp_path,
        should_process=False,
    )

    try:
        runner._paper_executor_snapshot = (
            lambda signal, state=None: (
                _snapshot(),
                False,
            )
        )

        runner._process_paper_executor(
            _signal(),
            "linear",
            "PRE_IMPULSE",
            h4_entry_context={
                "h4_entry_gate_allowed": True,
            },
        )

        row = store.get(KEY)

        assert row is not None
        assert row["initial_block_reason"] == (
            "entry_blocked_buy_flow"
        )

        assert len(captured) == 1
        assert captured[0]["decision"].action == WATCH
        assert captured[0]["context"][
            "deferred_entry_probe_only"
        ] is True
        assert captured[0]["context"][
            "deferred_entry_probe_reason"
        ] == "deferred_entry_probe_allowed"
        assert captured[0]["context"][
            "deferred_entry_registered"
        ] is True
    finally:
        store.close()


def test_runner_deferred_probe_never_opens_allowed_entry(
    tmp_path,
):
    runner, store, captured = _runner(
        tmp_path,
        should_process=False,
    )

    try:
        runner._paper_executor_snapshot = (
            lambda signal, state=None: (
                _snapshot(
                    buy_flow=130.0,
                    sell_flow=100.0,
                    volume_impulse=1.20,
                ),
                False,
            )
        )

        def fail_if_opened(*args, **kwargs):
            raise AssertionError(
                "deferred probe must not open a position"
            )

        runner._open_executor_position = fail_if_opened

        runner._process_paper_executor(
            _signal(),
            "linear",
            "PRE_IMPULSE",
            h4_entry_context={
                "h4_entry_gate_allowed": True,
            },
        )

        assert store.get(KEY) is None
        assert len(captured) == 1
        assert captured[0]["decision"].action == WATCH
        assert captured[0]["decision"].reason == (
            "deferred_entry_probe_entry_already_allowed"
        )
        assert captured[0]["context"][
            "deferred_entry_registered"
        ] is False
        assert captured[0]["context"][
            "deferred_entry_registration_reason"
        ] == (
            "deferred_entry_probe_entry_already_allowed"
        )
    finally:
        store.close()


def test_runner_without_deferred_runtime_keeps_standard_watch(
    tmp_path,
):
    runner, store, captured = _runner(tmp_path)

    try:
        runner.deferred_entry_runtime = None
        runner.deferred_entry_store = None

        runner._paper_executor_snapshot = (
            lambda signal, state=None: (
                _snapshot(),
                False,
            )
        )

        runner._process_paper_executor(
            _signal(),
            "linear",
            "PRE_IMPULSE",
            h4_entry_context={
                "h4_entry_gate_allowed": True,
            },
        )

        assert len(captured) == 1
        assert captured[0]["decision"].action == WATCH
        assert captured[0]["decision"].reason == (
            "entry_blocked_buy_flow"
        )
    finally:
        store.close()
