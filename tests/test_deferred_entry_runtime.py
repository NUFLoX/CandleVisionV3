from __future__ import annotations

from datetime import datetime, timezone

from orderflow_accum.deferred_entry import DeferredEntryStore
from orderflow_accum.deferred_entry_runtime import (
    DeferredEntryRuntime,
    DeferredEntryRuntimeConfig,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)


NOW = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)
KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"


def _runtime(
    tmp_path,
    *,
    enabled: bool = True,
) -> tuple[DeferredEntryRuntime, DeferredEntryStore]:
    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )

    coordinator = DeferredEntryCoordinator(store)

    runtime = DeferredEntryRuntime(
        coordinator,
        config=DeferredEntryRuntimeConfig(
            enabled=enabled,
            ttl_hours=24.0,
            h1_only=True,
        ),
    )

    return runtime, store


def _register(runtime: DeferredEntryRuntime, **overrides):
    values = {
        "mode": "paper",
        "signal_key": KEY,
        "symbol": "TESTUSDT",
        "market": "linear",
        "timeframe": "60",
        "side": "Buy",
        "signal_kind": "PRE_IMPULSE_ZONE",
        "score": 12.0,
        "origin_entry": 100.0,
        "origin_stop_loss": 90.0,
        "block_reason": "entry_blocked_buy_flow",
        "h4_allowed": True,
        "structural_allowed": True,
        "support": 94.0,
        "ema20": 96.0,
        "vwap": 96.5,
        "metadata": {
            "confirmed_status": "PRE_IMPULSE",
        },
        "now": NOW,
    }

    values.update(overrides)
    return runtime.register_blocked_setup(**values)


def test_registers_only_eligible_paper_h1_candidate(
    tmp_path,
):
    runtime, store = _runtime(tmp_path)

    try:
        result = _register(runtime)

        assert result.registered is True
        assert result.reason == "deferred_entry_created"

        row = store.get(KEY)

        assert row is not None
        assert row["initial_block_reason"] == (
            "entry_blocked_buy_flow"
        )
        assert row["metadata_json"][
            "confirmed_status"
        ] == "PRE_IMPULSE"
    finally:
        store.close()


def test_disabled_runtime_does_not_persist(tmp_path):
    runtime, store = _runtime(
        tmp_path,
        enabled=False,
    )

    try:
        result = _register(runtime)

        assert result.registered is False
        assert result.reason == "deferred_entry_disabled"
        assert store.get(KEY) is None
    finally:
        store.close()


def test_testnet_and_live_modes_are_rejected(tmp_path):
    runtime, store = _runtime(tmp_path)

    try:
        for mode in ("testnet", "live", ""):
            result = _register(runtime, mode=mode)

            assert result.registered is False
            assert result.reason == (
                "deferred_entry_mode_not_paper"
            )

        assert store.get(KEY) is None
    finally:
        store.close()


def test_structural_and_h4_blocks_are_rejected(tmp_path):
    runtime, store = _runtime(tmp_path)

    try:
        h4 = _register(
            runtime,
            h4_allowed=False,
        )
        structural = _register(
            runtime,
            structural_allowed=False,
        )
        rr = _register(
            runtime,
            block_reason="entry_blocked_bad_rr",
        )

        assert h4.reason == "deferred_entry_h4_gate_not_allowed"
        assert structural.reason == (
            "deferred_entry_structural_gate_not_allowed"
        )
        assert rr.reason == (
            "deferred_entry_block_reason_not_transient"
        )
        assert store.get(KEY) is None
    finally:
        store.close()


def test_non_h1_and_sell_candidates_are_rejected(tmp_path):
    runtime, store = _runtime(tmp_path)

    try:
        low_tf = _register(
            runtime,
            timeframe="15",
        )
        sell = _register(
            runtime,
            side="Sell",
        )

        assert low_tf.reason == (
            "deferred_entry_timeframe_not_h1"
        )
        assert sell.reason == "deferred_entry_not_buy_side"
        assert store.get(KEY) is None
    finally:
        store.close()
