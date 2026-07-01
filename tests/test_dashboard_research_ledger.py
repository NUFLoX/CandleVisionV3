from __future__ import annotations

from pathlib import Path

from dashboard.research_ledger import (
    list_research_runs,
    read_research_ledger,
)
from orderflow_accum.research_runs import (
    ResearchRunContext,
    ResearchRunLedger,
)
from orderflow_accum.signal_store import SignalStore


def _context() -> ResearchRunContext:
    return ResearchRunContext(
        strategy_id="accumulation_executor_baseline",
        strategy_version="v1",
        mode="paper",
        code_sha="dashboard-test-sha",
        config_hash="dashboard-config",
        config_json='{"dashboard":true}',
        label="dashboard-test",
    )


def test_missing_research_schema_returns_empty_payload(tmp_path) -> None:
    db_path = Path(tmp_path / "signals.db")
    store = SignalStore(str(db_path))

    try:
        payload = read_research_ledger(db_path)

        assert payload["scope"] == "research"
        assert payload["available"] is False
        assert payload["legacy_excluded"] is True
        assert payload["run"] is None
        assert payload["summary"]["total_closed_trades"] == 0
    finally:
        store.conn.close()


def test_research_reader_excludes_legacy_executor_trades(tmp_path) -> None:
    db_path = Path(tmp_path / "signals.db")
    store = SignalStore(str(db_path))

    try:
        # Legacy trade deliberately has a different result.
        store.upsert_executor_trade(
            {
                "trade_key": "legacy-trade",
                "signal_key": "legacy-signal",
                "symbol": "LEGACYUSDT",
                "side": "Buy",
                "state": "EXITED",
                "entry_price": 100.0,
                "exit_price": 5.0,
                "initial_sl": 95.0,
                "exit_reason": "legacy_loss",
                "r_result": -19.0,
            }
        )

        ledger = ResearchRunLedger(
            store.conn,
            enabled=True,
            context=_context(),
        )

        ledger.record_observation(
            signal_key="research-open-signal",
            symbol="OPENUSDT",
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
            diagnostics={},
        )

        ledger.record_trade(
            {
                "trade_key": "research-win",
                "signal_key": "research-closed-signal",
                "symbol": "RESEARCHUSDT",
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
                "entry_time": "2026-07-01T00:00:00+00:00",
                "exit_time": "2026-07-01T01:00:00+00:00",
                "diagnostics_json": {
                    "breakeven_time": "2026-07-01T00:30:00+00:00"
                },
            },
            signal_kind="PRE_IMPULSE_ZONE",
            btc_regime="BTC_NEUTRAL",
            market_regime="NEUTRAL",
        )

        payload = read_research_ledger(db_path)

        assert payload["scope"] == "research"
        assert payload["available"] is True
        assert payload["legacy_excluded"] is True
        assert payload["run"]["run_id"] == ledger.run_id

        assert payload["summary"]["total_open_trades"] == 1
        assert payload["summary"]["total_closed_trades"] == 1
        assert payload["summary"]["r_evaluated_trades"] == 1
        assert payload["summary"]["wins"] == 1
        assert payload["summary"]["net_r"] == 1.0
        assert payload["summary"]["avg_r"] == 1.0
        assert payload["summary"]["profit_factor"] is None

        assert len(payload["open_trades"]) == 1
        assert payload["open_trades"][0]["symbol"] == "OPENUSDT"

        assert len(payload["closed_trades"]) == 1
        assert payload["closed_trades"][0]["symbol"] == "RESEARCHUSDT"
        assert payload["closed_trades"][0]["r_result"] == 1.0
        assert (
            payload["closed_trades"][0]["moved_to_breakeven"]
            is True
        )

        assert len(payload["exit_reasons"]) == 1
        assert payload["exit_reasons"][0]["exit_reason"] == (
            "exit_take_profit"
        )
        assert payload["exit_reasons"][0]["avg_r"] == 1.0

        runs = list_research_runs(db_path)
        assert len(runs) == 1
        assert runs[0]["run_id"] == ledger.run_id
    finally:
        store.conn.close()
