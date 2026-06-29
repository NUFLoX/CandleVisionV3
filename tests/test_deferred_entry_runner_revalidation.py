from __future__ import annotations

import asyncio
from dataclasses import replace
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_PENDING,
    DEFERRED_ENTRY_READY,
    DeferredEntryCandidate,
    DeferredEntryStore,
)
from orderflow_accum.deferred_entry_revalidation_service import (
    DeferredEntryRevalidationService,
)
from orderflow_accum.deferred_entry_runtime import (
    DeferredEntryRuntime,
    DeferredEntryRuntimeConfig,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)
from orderflow_accum.runner import AccumulationRunner
from orderflow_accum.trade_executor import (
    ENTER_LONG,
    WATCH,
    OrderflowSnapshot,
    TradeDecision,
)


KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"
NOW = datetime(2026, 6, 29, tzinfo=timezone.utc)


class FakeLogger:
    def debug(self, *args, **kwargs):
        del args, kwargs

    def info(self, *args, **kwargs):
        del args, kwargs

    def exception(self, *args, **kwargs):
        del args, kwargs


class FakeStream:
    def __init__(self, state):
        self.state = state

    def get_state(self, symbol):
        del symbol
        return self.state


class FakeStrictExecutor:
    def __init__(self, decision):
        self.decision = decision
        self.calls = 0

    def evaluate_entry(self, setup, snapshot):
        del setup, snapshot
        self.calls += 1
        return self.decision


def _candidate() -> DeferredEntryCandidate:
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
        created_at=NOW,
        expires_at=NOW + timedelta(hours=24),
        origin_support=94.0,
        origin_ema20=96.0,
        origin_vwap=96.5,
        metadata={
            "source": "orderflow",
            "confirmed_status": "PRE_IMPULSE",
            "signal_meta": {
                "tf": "60",
                "market": "linear",
                "executor_snapshot": {
                    "price": 999.0,
                },
            },
            "signal_reasons": ["PRE_IMPULSE_ZONE"],
            "take_profit_1": 115.0,
            "take_profit_2": 130.0,
        },
    )


def _fresh_state(
    *,
    stale: bool = False,
    future: bool = False,
):
    timestamp = (
        time.time() + 3600.0
        if future
        else (
            time.time() - 3600.0
            if stale
            else time.time()
        )
    )

    return SimpleNamespace(
        snapshots=[
            SimpleNamespace(
                mid=97.0,
                spread_bps=3.0,
                ts=timestamp,
            )
        ],
        trades=[],
    )


def _snapshot() -> OrderflowSnapshot:
    return OrderflowSnapshot(
        price=97.0,
        spread_bps=3.0,
        buy_flow=150.0,
        sell_flow=100.0,
        bid_wall_strength=0.10,
        ask_wall_strength=0.20,
        volume_impulse=1.30,
        support=94.0,
        resistance=110.0,
        ema20=96.0,
        vwap=96.5,
        candle_close=97.0,
    )


def _structure() -> dict[str, object]:
    return {
        "price": 96.0,
        "candle_close": 96.0,
        "support": 94.0,
        "ema20": 96.0,
        "vwap": 96.5,
        "closed_h1_start": "2026-06-29T00:00:00Z",
    }


def _allowed_decision() -> TradeDecision:
    return TradeDecision(
        ENTER_LONG,
        "entry_allowed_long",
        "ENTERED",
        None,
    )


def _runner(
    tmp_path,
    *,
    mode: str = "paper",
    revalidation_enabled: bool = True,
    decision: TradeDecision | None = None,
):
    runner = AccumulationRunner.__new__(AccumulationRunner)

    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    runner.deferred_entry_store = store
    runner.deferred_entry_runtime = DeferredEntryRuntime(
        coordinator,
        config=DeferredEntryRuntimeConfig(
            enabled=True,
            ttl_hours=24.0,
            h1_only=True,
        ),
    )
    runner.deferred_entry_revalidation_service = (
        DeferredEntryRevalidationService(coordinator)
    )
    runner._deferred_entry_revalidation_enabled = (
        revalidation_enabled
    )
    runner.trade_executor_enabled = True
    runner.trade_executor_mode = mode
    runner.trade_executor = FakeStrictExecutor(
        decision or _allowed_decision()
    )
    runner.logger = FakeLogger()
    runner._env_float = lambda name, default: (
        0.0
        if name == (
            "EXECUTOR_DEFERRED_ENTRY_REVALIDATION_COOLDOWN_SECONDS"
        )
        else default
    )
    runner.regime_analyzer = SimpleNamespace(
        analyze_btc=lambda frames: SimpleNamespace(
            btc_regime="BTC_NEUTRAL",
            market_regime="BTC_NEUTRAL",
        )
    )

    async def fetch_btc(rest):
        del rest
        return {"60": object()}

    async def closed_h1(rest, record):
        del rest, record
        return _structure()

    async def h4_context(rest, signal):
        del rest, signal
        return {
            "h4_entry_gate_allowed": True,
            "h4_entry_gate_reason": (
                "h4_not_confirmed_bearish"
            ),
        }

    runner._fetch_btc_regime_frames = fetch_btc
    runner._deferred_entry_closed_h1_structure = closed_h1
    runner._h4_long_entry_gate_context = h4_context
    runner._paper_executor_snapshot = (
        lambda signal, state: (_snapshot(), False)
    )
    runner._derive_volume_impulse = (
        lambda *args, **kwargs: {
            "volume_impulse_missing": False,
            "volume_impulse_source": "test",
        }
    )
    runner._executor_symbol_blocked = lambda symbol: False
    runner._executor_target_quality_gate = (
        lambda *args, **kwargs: (None, {})
    )
    runner._entry_risk_reward_guard = (
        lambda *args, **kwargs: (None, {})
    )
    runner._executor_symbol_position_lock = (
        lambda *args, **kwargs: (None, {})
    )
    runner._evaluate_late_chase_gate = (
        lambda *args, **kwargs: (None, {})
    )
    runner._evaluate_executor_learning_gate = (
        lambda *args, **kwargs: (None, {})
    )
    runner._entry_stop_loss_guard = (
        lambda *args, **kwargs: (None, {})
    )

    registration = coordinator.register(_candidate())
    assert registration is not None

    store.conn.execute(
        """
        UPDATE deferred_entries
        SET status = ?
        WHERE signal_key = ?
        """,
        (DEFERRED_ENTRY_READY, KEY),
    )
    store.conn.commit()

    return runner, store


def test_ready_revalidation_persists_allow_without_opening(
    tmp_path,
):
    runner, store = _runner(tmp_path)

    def fail_if_called(*args, **kwargs):
        del args, kwargs
        raise AssertionError(
            "observe-only revalidation must not enter or override"
        )

    runner._open_executor_position = fail_if_called
    runner._executor_buy_momentum_override_decision = (
        fail_if_called
    )
    runner._evaluate_stop_reclaim_reentry = fail_if_called
    runner._evaluate_early_breakout_entry = fail_if_called

    try:
        persisted = asyncio.run(
            runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert persisted == 1
        assert runner.trade_executor.calls == 1

        row = store.get(KEY)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_READY
        assert row["revalidation_json"][
            "allowed_to_enter"
        ] is True
        assert row["revalidation_json"]["reason"] == (
            "deferred_entry_revalidation_strict_gates_passed"
        )
        assert row["revalidation_json"]["diagnostics"][
            "deferred_entry_revalidation_price"
        ] == 97.0
    finally:
        store.close()


def test_revalidation_blocks_stale_orderflow_before_executor(
    tmp_path,
):
    runner, store = _runner(tmp_path)

    def fail_if_orderflow_is_used(*args, **kwargs):
        del args, kwargs
        raise AssertionError(
            "stale orderflow must not reach executor snapshot"
        )

    runner._paper_executor_snapshot = fail_if_orderflow_is_used

    try:
        persisted = asyncio.run(
            runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(
                    _fresh_state(stale=True)
                ),
            )
        )

        assert persisted == 1
        assert runner.trade_executor.calls == 0

        row = store.get(KEY)
        assert row is not None
        assert row["revalidation_json"]["reason"] == (
            "deferred_entry_revalidation_missing_fresh_orderflow"
        )
    finally:
        store.close()


def test_revalidation_blocks_h4_before_executor(
    tmp_path,
):
    runner, store = _runner(tmp_path)

    async def bearish_h4(rest, signal):
        del rest, signal
        return {
            "h4_entry_gate_allowed": False,
            "h4_entry_gate_reason": (
                "entry_blocked_h4_bearish_structure"
            ),
        }

    runner._h4_long_entry_gate_context = bearish_h4

    try:
        persisted = asyncio.run(
            runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert persisted == 1
        assert runner.trade_executor.calls == 0

        row = store.get(KEY)
        assert row is not None
        assert row["revalidation_json"]["reason"] == (
            "entry_blocked_h4_bearish_structure"
        )
    finally:
        store.close()


def test_revalidation_does_nothing_when_disabled_or_not_paper(
    tmp_path,
):
    disabled_runner, disabled_store = _runner(
        tmp_path / "disabled",
        revalidation_enabled=False,
    )

    try:
        persisted = asyncio.run(
            disabled_runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert persisted == 0
        assert disabled_runner.trade_executor.calls == 0

        row = disabled_store.get(KEY)
        assert row is not None
        assert row["revalidation_json"] == {}
    finally:
        disabled_store.close()

    paper_runner, paper_store = _runner(
        tmp_path / "testnet",
        mode="testnet",
    )

    try:
        persisted = asyncio.run(
            paper_runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert persisted == 0
        assert paper_runner.trade_executor.calls == 0

        row = paper_store.get(KEY)
        assert row is not None
        assert row["revalidation_json"] == {}
    finally:
        paper_store.close()

def test_revalidation_blocks_future_orderflow_before_executor(
    tmp_path,
):
    runner, store = _runner(tmp_path)

    def fail_if_orderflow_is_used(*args, **kwargs):
        del args, kwargs
        raise AssertionError(
            "future orderflow must not reach executor snapshot"
        )

    runner._paper_executor_snapshot = fail_if_orderflow_is_used

    try:
        persisted = asyncio.run(
            runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(
                    _fresh_state(future=True)
                ),
            )
        )

        assert persisted == 1
        assert runner.trade_executor.calls == 0

        row = store.get(KEY)
        assert row is not None
        assert row["revalidation_json"]["reason"] == (
            "deferred_entry_revalidation_missing_fresh_orderflow"
        )
        assert row["revalidation_json"]["diagnostics"][
            "deferred_entry_revalidation_orderflow_age_seconds"
        ] < 0
    finally:
        store.close()

def test_revalidation_respects_active_candidate_limit(
    tmp_path,
):
    runner, store = _runner(tmp_path)

    runner._env_float = lambda name, default: (
        1.0
        if name == (
            "EXECUTOR_DEFERRED_ENTRY_REVALIDATION_MAX_ACTIVE"
        )
        else (
            0.0
            if name == (
                "EXECUTOR_DEFERRED_ENTRY_REVALIDATION_COOLDOWN_SECONDS"
            )
            else default
        )
    )

    second = replace(
        _candidate(),
        signal_key=(
            "TEST2USDT|linear|60|PRE_IMPULSE_ZONE|Buy"
        ),
        symbol="TEST2USDT",
    )

    try:
        registration = (
            runner.deferred_entry_runtime.coordinator.register(
                second
            )
        )
        assert registration is not None

        store.conn.execute(
            """
            UPDATE deferred_entries
            SET status = ?
            WHERE signal_key = ?
            """,
            (
                DEFERRED_ENTRY_READY,
                second.signal_key,
            ),
        )
        store.conn.commit()

        persisted = asyncio.run(
            runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert persisted == 1
        assert runner.trade_executor.calls == 1

        rows = [
            store.get(KEY),
            store.get(second.signal_key),
        ]

        assert sum(
            int(bool(row["revalidation_json"]))
            for row in rows
            if row is not None
        ) == 1
    finally:
        store.close()

def test_revalidation_filters_ready_before_active_bound(
    tmp_path,
):
    runner, store = _runner(tmp_path)

    runner._env_float = lambda name, default: (
        1.0
        if name == (
            "EXECUTOR_DEFERRED_ENTRY_REVALIDATION_MAX_ACTIVE"
        )
        else (
            0.0
            if name == (
                "EXECUTOR_DEFERRED_ENTRY_REVALIDATION_COOLDOWN_SECONDS"
            )
            else default
        )
    )

    pending = replace(
        _candidate(),
        signal_key=(
            "PENDINGUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"
        ),
        symbol="PENDINGUSDT",
    )

    try:
        registration = (
            runner.deferred_entry_runtime.coordinator.register(
                pending
            )
        )
        assert registration is not None

        store.conn.execute(
            """
            UPDATE deferred_entries
            SET status = ?, updated_at = ?
            WHERE signal_key = ?
            """,
            (
                DEFERRED_ENTRY_PENDING,
                "2000-01-01T00:00:00+00:00",
                pending.signal_key,
            ),
        )
        store.conn.execute(
            """
            UPDATE deferred_entries
            SET status = ?, updated_at = ?
            WHERE signal_key = ?
            """,
            (
                DEFERRED_ENTRY_READY,
                "2100-01-01T00:00:00+00:00",
                KEY,
            ),
        )
        store.conn.commit()

        persisted = asyncio.run(
            runner.revalidate_ready_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert persisted == 1
        assert runner.trade_executor.calls == 1

        ready_row = store.get(KEY)
        pending_row = store.get(pending.signal_key)

        assert ready_row is not None
        assert pending_row is not None
        assert ready_row["status"] == DEFERRED_ENTRY_READY
        assert ready_row["revalidation_json"][
            "allowed_to_enter"
        ] is True
        assert pending_row["status"] == DEFERRED_ENTRY_PENDING
        assert pending_row["revalidation_json"] == {}
    finally:
        store.close()
