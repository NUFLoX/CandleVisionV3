from __future__ import annotations

from types import SimpleNamespace

from orderflow_accum.deferred_entry import (
    DeferredEntryStore,
)
from orderflow_accum.deferred_entry_runner_adapter import (
    register_deferred_watch,
)
from orderflow_accum.deferred_entry_runtime import (
    DeferredEntryRuntime,
    DeferredEntryRuntimeConfig,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)
from orderflow_accum.trade_executor import (
    OrderflowSnapshot,
    TradeSetup,
)


KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"


def _runtime(tmp_path) -> tuple[DeferredEntryRuntime, DeferredEntryStore]:
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )

    runtime = DeferredEntryRuntime(
        DeferredEntryCoordinator(store),
        config=DeferredEntryRuntimeConfig(
            enabled=True,
            ttl_hours=24.0,
            h1_only=True,
        ),
    )

    return runtime, store


def _signal():
    return SimpleNamespace(
        source="orderflow",
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


def _snapshot():
    return OrderflowSnapshot(
        price=100.0,
        spread_bps=4.0,
        buy_flow=100.0,
        sell_flow=100.0,
        bid_wall_strength=0.10,
        ask_wall_strength=0.20,
        volume_impulse=0.50,
        support=94.0,
        resistance=115.0,
        ema20=96.0,
        vwap=96.5,
    )


def test_registers_transient_watch_with_full_context(tmp_path):
    runtime, store = _runtime(tmp_path)

    try:
        result = register_deferred_watch(
            runtime=runtime,
            mode="paper",
            signal_key=KEY,
            signal=_signal(),
            setup=_setup(),
            snapshot=_snapshot(),
            market="linear",
            block_reason="entry_blocked_buy_flow",
            confirmed_status="PRE_IMPULSE",
            h4_allowed=True,
            structural_allowed=True,
            structural_blockers=[],
        )

        assert result["deferred_entry_registered"] is True
        assert result["deferred_entry_registration_reason"] == (
            "deferred_entry_created"
        )

        row = store.get(KEY)

        assert row is not None
        assert row["metadata_json"]["take_profit_1"] == 115.0
        assert row["metadata_json"]["initial_snapshot"][
            "volume_impulse"
        ] == 0.50
    finally:
        store.close()


def test_rejects_h4_or_structural_failure(tmp_path):
    runtime, store = _runtime(tmp_path)

    try:
        h4_result = register_deferred_watch(
            runtime=runtime,
            mode="paper",
            signal_key=KEY,
            signal=_signal(),
            setup=_setup(),
            snapshot=_snapshot(),
            market="linear",
            block_reason="entry_blocked_buy_flow",
            confirmed_status="PRE_IMPULSE",
            h4_allowed=False,
            structural_allowed=True,
            structural_blockers=[],
        )

        assert h4_result["deferred_entry_registered"] is False
        assert h4_result[
            "deferred_entry_registration_reason"
        ] == "deferred_entry_h4_gate_not_allowed"

        structural_result = register_deferred_watch(
            runtime=runtime,
            mode="paper",
            signal_key=KEY,
            signal=_signal(),
            setup=_setup(),
            snapshot=_snapshot(),
            market="linear",
            block_reason="entry_blocked_volume_impulse",
            confirmed_status="PRE_IMPULSE",
            h4_allowed=True,
            structural_allowed=False,
            structural_blockers=[
                "entry_blocked_below_support",
            ],
        )

        assert structural_result[
            "deferred_entry_registered"
        ] is False
        assert structural_result[
            "deferred_entry_registration_reason"
        ] == "deferred_entry_structural_gate_not_allowed"

        assert store.get(KEY) is None
    finally:
        store.close()
