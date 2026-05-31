from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import ModuleType

chart_render_stub = ModuleType("orderflow_accum.chart_render")
chart_render_stub.render_signal_chart = lambda *args, **kwargs: None
sys.modules.setdefault("orderflow_accum.chart_render", chart_render_stub)

from orderflow_accum.models import Signal
from orderflow_accum.runner import AccumulationRunner
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_executor import SmartTradeExecutor


class DummySettings:
    market_categories = ["linear"]


def make_signal(**overrides) -> Signal:
    data = {
        "symbol": "ETHUSDT",
        "side": "Buy",
        "kind": "CONFIRMED_LONG",
        "source": "orderflow",
        "score": 9.0,
        "entry": 100.0,
        "stop_loss": 99.0,
        "take_profit_1": 102.0,
        "take_profit_2": 104.0,
        "reasons": ["long_promotion_rules_met"],
        "meta": {"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL"},
    }
    data.update(overrides)
    return Signal(**data)


def make_snapshot(**overrides) -> dict[str, float | int | None]:
    data: dict[str, float | int | None] = {
        "price": 100.0,
        "spread_bps": 4.0,
        "buy_flow": 140.0,
        "sell_flow": 90.0,
        "bid_wall_strength": 0.2,
        "ask_wall_strength": 0.2,
        "volume_impulse": 1.4,
        "support": None,
        "resistance": 102.0,
        "ema20": None,
        "vwap": None,
        "bars_since_entry": 0,
    }
    data.update(overrides)
    return data


def make_runner(tmp_path: Path, *, enabled: bool = True, mode: str = "paper") -> AccumulationRunner:
    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.settings = DummySettings()
    runner.logger = logging.getLogger("test.paper_executor")
    runner.signal_store = SignalStore(db_path=str(tmp_path / "signals.db"))
    runner.trade_executor_mode = mode
    runner.trade_executor_enabled = enabled and mode == "paper"
    runner.trade_executor = SmartTradeExecutor() if runner.trade_executor_enabled else None
    return runner


def row_count(store: SignalStore) -> int:
    return int(store.conn.execute("SELECT COUNT(*) FROM executor_outcomes").fetchone()[0])


def signal_key(signal: Signal) -> str:
    return f"{signal.symbol}|linear|{signal.meta['tf']}|{signal.kind}|{signal.side}"


def test_run_trade_executor_disabled_creates_no_executor_rows(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=False)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    assert row_count(runner.signal_store) == 0
    runner.signal_store.close()


def test_confirmed_long_missing_snapshot_creates_trade_watch_row(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal()

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert row is not None
    assert row["state"] == "TRADE_WATCH"
    assert row["action"] == "WATCH"
    assert row["reason"] == "paper_executor_missing_snapshot_data"
    runner.signal_store.close()


def test_valid_long_snapshot_enters_paper_state(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert row is not None
    assert row["state"] == "ENTERED"
    assert row["action"] == "ENTER_LONG"
    assert row["reason"] == "entry_allowed_long"
    assert row["entry_price"] == 100.0
    runner.signal_store.close()


def test_half_r_confirmation_moves_stop_to_breakeven(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.6, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.1)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "MOVE_SL_TO_BREAKEVEN"
    assert row["state"] == "PROTECT_BREAKEVEN"
    assert row["current_sl"] > 100.0
    runner.signal_store.close()


def test_sell_flow_dominance_exits_long_paper_position(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.1, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "EXIT"
    assert row["state"] == "EXITED"
    assert row["exit_reason"] == "exit_sell_flow_dominance"
    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["signal_key"] == key
    assert trades[0]["exit_action"] == "EXIT"
    assert round(float(trades[0]["r_result"]), 6) == 0.1
    runner.signal_store.close()


def test_breakeven_exit_stores_moved_to_breakeven_in_executor_trade(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.6, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.1)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.2, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["moved_to_breakeven"] == 1
    runner.signal_store.close()


def test_confirmed_short_runs_only_in_paper_mode(tmp_path: Path) -> None:
    live_runner = make_runner(tmp_path / "live", enabled=True, mode="live")
    short_signal = make_signal(
        side="Sell",
        kind="CONFIRMED_SHORT",
        stop_loss=101.0,
        take_profit_1=98.0,
        take_profit_2=96.0,
        reasons=["short_promotion_rules_met"],
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_BEARISH",
            "executor_snapshot": make_snapshot(
                buy_flow=80.0,
                sell_flow=140.0,
                bid_wall_strength=0.2,
                resistance=101.0,
                ema20=100.5,
                vwap=100.4,
            ),
        },
    )
    live_runner._process_paper_executor(short_signal, "linear", "CONFIRMED_SHORT")
    assert row_count(live_runner.signal_store) == 0
    live_runner.signal_store.close()

    paper_runner = make_runner(tmp_path / "paper", enabled=True, mode="paper")
    paper_runner._process_paper_executor(short_signal, "linear", "CONFIRMED_SHORT")
    row = paper_runner.signal_store.get_executor_outcome(signal_key(short_signal))
    assert row is not None
    assert row["action"] == "ENTER_SHORT"
    assert row["state"] == "ENTERED"
    paper_runner.signal_store.close()


def test_missing_snapshot_fields_do_not_crash(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": {}})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert row is not None
    assert row["action"] in {"WATCH", "ENTER_LONG"}
    runner.signal_store.close()


def test_executor_state_does_not_overwrite_signal_status_or_outcome(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    runner.signal_store.upsert_signal(signal, market="linear")
    runner.signal_store.conn.execute(
        "UPDATE signals SET outcome = 'PENDING' WHERE signal_key = ?",
        (signal_key(signal),),
    )
    runner.signal_store.conn.commit()

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.conn.execute(
        "SELECT status, outcome FROM signals WHERE signal_key = ?",
        (signal_key(signal),),
    ).fetchone()
    assert row["status"] == "CONFIRMED_LONG"
    assert row["outcome"] == "PENDING"
    runner.signal_store.close()


def test_run_accumulation_bat_contains_executor_defaults() -> None:
    text = Path("run_accumulation_v1.bat").read_text(encoding="utf-8")

    assert 'set "RUN_TRADE_EXECUTOR=true"' in text
    assert 'set "TRADE_EXECUTOR_MODE=paper"' in text
    assert "echo RUN_TRADE_EXECUTOR=%RUN_TRADE_EXECUTOR%" in text
    assert "echo TRADE_EXECUTOR_MODE=%TRADE_EXECUTOR_MODE%" in text
