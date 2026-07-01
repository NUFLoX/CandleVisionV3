from __future__ import annotations

from orderflow_accum.research_runs import (
    ResearchRunContext,
    ResearchRunLedger,
)
from orderflow_accum.signal_store import SignalStore


def _context(config_hash: str = "config-a") -> ResearchRunContext:
    return ResearchRunContext(
        strategy_id="accumulation_executor_baseline",
        strategy_version="v1",
        mode="paper",
        code_sha="test-sha",
        config_hash=config_hash,
        config_json='{"test":true}',
        label="pytest",
    )


def _table_exists(store: SignalStore, table_name: str) -> bool:
    row = store.conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def test_disabled_ledger_has_no_schema_side_effect(tmp_path) -> None:
    store = SignalStore(str(tmp_path / "signals.db"))

    try:
        ledger = ResearchRunLedger(
            store.conn,
            enabled=False,
            context=_context(),
        )

        assert ledger.run_id is None
        assert _table_exists(store, "research_runs") is False
        assert _table_exists(
            store,
            "research_executor_observations",
        ) is False
        assert _table_exists(
            store,
            "research_executor_trades",
        ) is False
    finally:
        store.conn.close()


def test_same_context_reuses_active_research_run(tmp_path) -> None:
    store = SignalStore(str(tmp_path / "signals.db"))

    try:
        first = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context("config-a"),
        )
        second = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context("config-a"),
        )
        changed = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context("config-b"),
        )

        assert first.run_id is not None
        assert first.run_id == second.run_id
        assert changed.run_id is not None
        assert changed.run_id != first.run_id

        rows = store.conn.execute(
            """
            SELECT run_id, status
            FROM research_runs
            """
        ).fetchall()

        statuses = {
            str(row["run_id"]): str(row["status"])
            for row in rows
        }

        assert statuses[first.run_id] == "SUPERSEDED"
        assert statuses[changed.run_id] == "ACTIVE"
    finally:
        store.conn.close()


def test_research_rows_are_isolated_from_legacy_executor_tables(tmp_path) -> None:
    store = SignalStore(str(tmp_path / "signals.db"))

    try:
        ledger = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context(),
        )

        ledger.record_observation(
            signal_key="research-signal",
            symbol="TESTUSDT",
            timeframe="60",
            side="Buy",
            state="ENTERED",
            action="ENTER_LONG",
            reason="entry_allowed_long",
            entry_price=100.0,
            current_sl=95.0,
            max_gain_r=0.5,
            max_drawdown_r=0.1,
            bars_in_trade=2,
            signal_kind="PRE_IMPULSE_ZONE",
            btc_regime="BTC_NEUTRAL",
            market_regime="NEUTRAL",
            diagnostics={"source": "pytest"},
        )

        ledger.record_trade(
            {
                "trade_key": "research-trade",
                "signal_key": "research-signal",
                "symbol": "TESTUSDT",
                "timeframe": "60",
                "side": "Buy",
                "state": "EXITED",
                "entry_price": 100.0,
                "exit_price": 105.0,
                "initial_sl": 95.0,
                "final_sl": 95.0,
                "exit_reason": "exit_take_profit",
                "r_result": 1.0,
                "max_gain_r": 1.2,
                "max_drawdown_r": 0.1,
                "bars_in_trade": 3,
                "duration_minutes": 60.0,
                "moved_to_breakeven": True,
                "entry_time": "2026-07-01T00:00:00+00:00",
                "exit_time": "2026-07-01T01:00:00+00:00",
                "diagnostics_json": {"source": "pytest"},
            },
            signal_kind="PRE_IMPULSE_ZONE",
            btc_regime="BTC_NEUTRAL",
            market_regime="NEUTRAL",
        )

        observation = store.conn.execute(
            """
            SELECT *
            FROM research_executor_observations
            WHERE signal_key = ?
            """,
            ("research-signal",),
        ).fetchone()

        trade = store.conn.execute(
            """
            SELECT *
            FROM research_executor_trades
            WHERE trade_key = ?
            """,
            ("research-trade",),
        ).fetchone()

        legacy_outcome_columns = {
            str(row["name"])
            for row in store.conn.execute(
                "PRAGMA table_info(executor_outcomes)"
            ).fetchall()
        }

        legacy_trade_columns = {
            str(row["name"])
            for row in store.conn.execute(
                "PRAGMA table_info(executor_trades)"
            ).fetchall()
        }

        assert observation is not None
        assert trade is not None
        assert observation["run_id"] == ledger.run_id
        assert trade["run_id"] == ledger.run_id
        assert "research_run_id" not in legacy_outcome_columns
        assert "research_run_id" not in legacy_trade_columns
    finally:
        store.conn.close()
