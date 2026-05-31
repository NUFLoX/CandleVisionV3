from __future__ import annotations

from pathlib import Path

from orderflow_accum.signal_store import SignalStore


def test_executor_trades_schema_is_created(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))

    table = store.conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'executor_trades'"
    ).fetchone()
    indexes = {
        row["name"]
        for row in store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'executor_trades'"
        ).fetchall()
    }

    assert table is not None
    assert {
        "idx_executor_trades_signal_key",
        "idx_executor_trades_symbol_timeframe",
        "idx_executor_trades_exit_reason",
        "idx_executor_trades_exit_time",
        "idx_executor_trades_r_result",
    }.issubset(indexes)
    store.close()


def test_upsert_executor_trade_deduplicates_by_trade_key(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    trade = {
        "trade_key": "sig|entry|exit",
        "signal_key": "sig",
        "symbol": "BTCUSDT",
        "timeframe": "5",
        "side": "Buy",
        "state": "EXITED",
        "entry_action": "ENTER_LONG",
        "exit_action": "EXIT",
        "entry_price": 100.0,
        "exit_price": 102.0,
        "initial_sl": 99.0,
        "current_sl": 100.1,
        "exit_reason": "exit_sell_flow_dominance",
        "r_result": 2.0,
        "moved_to_breakeven": False,
    }

    store.upsert_executor_trade(trade)
    store.upsert_executor_trade({**trade, "r_result": 3.0, "moved_to_breakeven": True})

    rows = store.list_executor_trades()
    assert len(rows) == 1
    assert rows[0]["trade_key"] == "sig|entry|exit"
    assert rows[0]["r_result"] == 3.0
    assert rows[0]["moved_to_breakeven"] == 1
    assert store.get_executor_trade("sig|entry|exit") is not None
    store.close()
