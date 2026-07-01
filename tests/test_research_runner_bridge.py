from __future__ import annotations

from types import SimpleNamespace

from orderflow_accum.research_runner_bridge import (
    copy_executor_observation,
    copy_executor_trade,
)
from orderflow_accum.research_runs import (
    ResearchRunContext,
    ResearchRunLedger,
)
from orderflow_accum.signal_store import SignalStore


FUTURE_TIME = "2999-07-01T00:00:00+00:00"
LEGACY_TIME = "2000-01-01T00:00:00+00:00"


def _context() -> ResearchRunContext:
    return ResearchRunContext(
        strategy_id="accumulation_executor_baseline",
        strategy_version="v1",
        mode="paper",
        code_sha="test-sha",
        config_hash="bridge-config",
        config_json='{"test":true}',
        label="bridge-test",
    )


def _signal(symbol: str = "TESTUSDT") -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        side="Buy",
        kind="PRE_IMPULSE_ZONE",
        meta={
            "tf": "60",
            "btc_regime": "BTC_NEUTRAL",
            "market_regime": "NEUTRAL",
        },
    )


def _count_rows(
    store: SignalStore,
    table_name: str,
    run_id: str,
) -> int:
    row = store.conn.execute(
        f"SELECT COUNT(*) AS count FROM {table_name} WHERE run_id = ?",
        (run_id,),
    ).fetchone()

    return int(row["count"])


def test_bridge_copies_member_entry_and_closed_trade(tmp_path) -> None:
    store = SignalStore(str(tmp_path / "signals.db"))

    try:
        ledger = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context(),
        )
        signal = _signal()

        copy_executor_observation(
            ledger,
            signal_key="bridge-signal",
            signal=signal,
            row={
                "created_at": FUTURE_TIME,
                "side": "Buy",
                "state": "ENTERED",
                "action": "ENTER_LONG",
                "reason": "entry_allowed_long",
                "entry_price": 100.0,
                "current_sl": 95.0,
                "exit_price": None,
                "exit_reason": None,
                "max_gain_r": 0.5,
                "max_drawdown_r": 0.1,
                "bars_in_trade": 2,
            },
            diagnostics_json={
                "executor_timeframe": "60",
                "signal_kind": "PRE_IMPULSE_ZONE",
                "btc_regime": "BTC_NEUTRAL",
                "market_regime": "NEUTRAL",
            },
        )

        copy_executor_trade(
            ledger,
            signal=signal,
            diagnostics_json={},
            trade={
                "trade_key": "bridge-trade",
                "signal_key": "bridge-signal",
                "symbol": "TESTUSDT",
                "created_at": FUTURE_TIME,
                "timeframe": "60",
                "side": "Buy",
                "state": "EXITED",
                "entry_price": 100.0,
                "exit_price": 105.0,
                "initial_sl": 95.0,
                "final_sl": 100.0,
                "exit_reason": "exit_take_profit",
                "r_result": 1.0,
                "max_gain_r": 1.2,
                "max_drawdown_r": 0.1,
                "bars_in_trade": 3,
                "duration_minutes": 60.0,
                "moved_to_breakeven": True,
                "entry_time": FUTURE_TIME,
                "exit_time": FUTURE_TIME,
            },
        )

        observation = store.conn.execute(
            "SELECT symbol, action, signal_kind "
            "FROM research_executor_observations "
            "WHERE run_id = ? AND signal_key = ?",
            (ledger.run_id, "bridge-signal"),
        ).fetchone()

        trade = store.conn.execute(
            "SELECT symbol, r_result, moved_to_breakeven "
            "FROM research_executor_trades "
            "WHERE run_id = ? AND trade_key = ?",
            (ledger.run_id, "bridge-trade"),
        ).fetchone()

        membership_count = _count_rows(
            store,
            "research_run_signal_membership",
            ledger.run_id,
        )

        assert observation is not None
        assert observation["symbol"] == "TESTUSDT"
        assert observation["action"] == "ENTER_LONG"
        assert observation["signal_kind"] == "PRE_IMPULSE_ZONE"

        assert trade is not None
        assert trade["symbol"] == "TESTUSDT"
        assert trade["r_result"] == 1.0
        assert trade["moved_to_breakeven"] == 1
        assert membership_count == 1
    finally:
        store.conn.close()


def test_bridge_excludes_pre_run_legacy_rows(tmp_path) -> None:
    store = SignalStore(str(tmp_path / "signals.db"))

    try:
        ledger = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context(),
        )
        signal = _signal("LEGACYUSDT")

        copy_executor_observation(
            ledger,
            signal_key="legacy-signal",
            signal=signal,
            row={
                "created_at": LEGACY_TIME,
                "action": "ENTER_LONG",
            },
            diagnostics_json={},
        )

        copy_executor_trade(
            ledger,
            signal=signal,
            diagnostics_json={},
            trade={
                "trade_key": "legacy-trade",
                "signal_key": "legacy-signal",
                "symbol": "LEGACYUSDT",
                "created_at": LEGACY_TIME,
            },
        )

        assert _count_rows(
            store,
            "research_executor_observations",
            ledger.run_id,
        ) == 0

        assert _count_rows(
            store,
            "research_executor_trades",
            ledger.run_id,
        ) == 0
    finally:
        store.conn.close()


def test_bridge_rejects_post_start_nonmember_rows(tmp_path) -> None:
    store = SignalStore(str(tmp_path / "signals.db"))

    try:
        ledger = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context(),
        )
        signal = _signal("POSTSTARTUSDT")

        copy_executor_observation(
            ledger,
            signal_key="poststart-nonmember-signal",
            signal=signal,
            row={
                "created_at": FUTURE_TIME,
                "state": "ENTERED",
                "action": "HOLD",
            },
            diagnostics_json={},
        )

        copy_executor_trade(
            ledger,
            signal=signal,
            diagnostics_json={},
            trade={
                "trade_key": "poststart-nonmember-trade",
                "signal_key": "poststart-nonmember-signal",
                "symbol": "POSTSTARTUSDT",
                "created_at": FUTURE_TIME,
            },
        )

        assert _count_rows(
            store,
            "research_executor_observations",
            ledger.run_id,
        ) == 0

        assert _count_rows(
            store,
            "research_executor_trades",
            ledger.run_id,
        ) == 0
    finally:
        store.conn.close()
