from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from types import ModuleType


def _stub_module(name: str, **attrs) -> None:
    module = ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules.setdefault(name, module)


class _StubService:
    def __init__(self, *args, **kwargs):
        pass


class _StubBybitRestClient(_StubService):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass


class _StubScanTarget:
    def __init__(self, symbol: str = "", market: str = "") -> None:
        self.symbol = symbol
        self.market = market


_stub_module("numpy")
_stub_module("pandas")
_stub_module("dashboard.ingest_client", DashboardIngestClient=_StubService)
_stub_module("orderflow_accum.bybit_rest", BybitRestClient=_StubBybitRestClient, ScanTarget=_StubScanTarget)
_stub_module("orderflow_accum.console_ui", ConsoleUI=_StubService)
_stub_module("orderflow_accum.engines", MacroAccumulationEngine=_StubService, RealtimeAccumulationEngine=_StubService)
_stub_module("orderflow_accum.short_engine", DistributionShortEngine=_StubService)
_stub_module("orderflow_accum.market_regime", MarketRegimeAnalyzer=_StubService)
_stub_module("orderflow_accum.signal_logger", RejectionCsvLogger=_StubService, SignalCsvLogger=_StubService)
_stub_module("orderflow_accum.confirmed_promoter", ConfirmedPromoter=_StubService)
_stub_module("orderflow_accum.telegram_notify", TelegramNotifier=_StubService)
_stub_module("orderflow_accum.ws_clients", MarketStream=_StubService)
_stub_module("orderflow_accum.chart_render", render_signal_chart=lambda *args, **kwargs: None)

from orderflow_accum.models import Signal
from orderflow_accum.runner import AccumulationRunner
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_executor import SmartTradeExecutor
from orderflow_accum.trade_learning import TradeLearningEngine


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
    key = signal_key(signal)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["state"] == "ENTERED"
    assert row["action"] == "ENTER_LONG"
    assert row["reason"] == "entry_allowed_long"
    assert row["entry_price"] == 100.0
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["executor_entry_time"]
    assert diagnostics["executor_entry_price"] == 100.0
    assert diagnostics["executor_initial_sl"] == 99.0
    assert diagnostics["executor_side"] == "Buy"
    assert diagnostics["executor_signal_key"] == key
    assert diagnostics["executor_timeframe"] == "5"
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


def test_max_gain_r_fallback_moves_long_stop_to_breakeven_when_snapshot_flow_is_weak(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET max_gain_r = ? WHERE signal_key = ?",
        (1.2, key),
    )
    runner.signal_store.conn.commit()

    signal.meta["executor_snapshot"] = make_snapshot(price=100.2, buy_flow=100.0, sell_flow=110.0, volume_impulse=0.5)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "MOVE_SL_TO_BREAKEVEN"
    assert row["reason"] == "sl_moved_to_breakeven_after_max_r"
    assert row["state"] == "PROTECT_BREAKEVEN"
    assert row["current_sl"] > 100.0
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["executor_entry_price"] == 100.0
    assert diagnostics["executor_initial_sl"] == 99.0
    assert diagnostics["breakeven_time"]
    runner.signal_store.close()


def test_max_gain_r_below_one_holds_long_when_snapshot_flow_is_weak(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET max_gain_r = ? WHERE signal_key = ?",
        (0.99, key),
    )
    runner.signal_store.conn.commit()

    signal.meta["executor_snapshot"] = make_snapshot(price=100.2, buy_flow=100.0, sell_flow=110.0, volume_impulse=0.5)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "HOLD"
    assert row["reason"] == "hold_position"
    assert row["state"] == "ENTERED"
    assert row["current_sl"] == 99.0
    runner.signal_store.close()


def test_already_protect_breakeven_does_not_duplicate_breakeven_event(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    runner.signal_store.conn.execute(
        """
        UPDATE executor_outcomes
        SET state = ?, action = ?, reason = ?, current_sl = ?, max_gain_r = ?
        WHERE signal_key = ?
        """,
        ("PROTECT_BREAKEVEN", "MOVE_SL_TO_BREAKEVEN", "sl_moved_to_breakeven", 100.1, 1.2, key),
    )
    runner.signal_store.conn.commit()

    signal.meta["executor_snapshot"] = make_snapshot(price=100.2, buy_flow=100.0, sell_flow=110.0, volume_impulse=0.5)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "HOLD"
    assert row["reason"] == "hold_position"
    assert row["state"] == "PROTECT_BREAKEVEN"
    assert row["current_sl"] == 100.1
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
    assert trades[0]["entry_price"] == 100.0
    assert trades[0]["initial_sl"] == 99.0
    assert round(float(trades[0]["r_result"]), 6) == 0.1
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["executor_entry_time"] == trades[0]["entry_time"]
    runner.signal_store.close()


def test_long_stop_loss_exit_uses_current_sl_for_ledger_price_and_r(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=95.0, buy_flow=140.0, sell_flow=90.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["exit_reason"] == "exit_stop_loss_hit"
    assert row["exit_price"] == 95.0
    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["exit_price"] == 99.0
    assert trades[0]["current_sl"] == 99.0
    assert round(float(trades[0]["r_result"]), 6) == -1.0
    diagnostics = json.loads(trades[0]["diagnostics_json"])
    assert diagnostics["observed_exit_price"] == 95.0
    assert diagnostics["stop_execution_price"] == 99.0
    runner.signal_store.close()


def test_long_stop_loss_after_breakeven_uses_current_sl_and_not_minus_one_r(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.6, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.1)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    breakeven_row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert breakeven_row is not None
    breakeven_sl = float(breakeven_row["current_sl"])

    signal.meta["executor_snapshot"] = make_snapshot(price=99.0, buy_flow=140.0, sell_flow=90.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["exit_reason"] == "exit_stop_loss_hit"
    assert trades[0]["exit_price"] == breakeven_sl
    assert float(trades[0]["r_result"]) > 0.0
    assert round(float(trades[0]["r_result"]), 6) != -1.0
    diagnostics = json.loads(trades[0]["diagnostics_json"])
    assert diagnostics["observed_exit_price"] == 99.0
    assert diagnostics["stop_execution_price"] == breakeven_sl
    runner.signal_store.close()


def test_non_stop_exit_uses_observed_exit_price_in_ledger(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.1, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["exit_reason"] == "exit_sell_flow_dominance"
    assert trades[0]["exit_price"] == 100.1
    diagnostics = json.loads(trades[0]["diagnostics_json"])
    assert "observed_exit_price" not in diagnostics
    assert "stop_execution_price" not in diagnostics
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
    assert trades[0]["breakeven_time"]
    runner.signal_store.close()


def test_hold_preserves_executor_entry_snapshot(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    entry_row = runner.signal_store.get_executor_outcome(key)
    assert entry_row is not None
    entry_diagnostics = json.loads(entry_row["diagnostics_json"])

    signal.meta["executor_snapshot"] = make_snapshot(price=100.2, buy_flow=130.0, sell_flow=100.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "HOLD"
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["executor_entry_time"] == entry_diagnostics["executor_entry_time"]
    assert diagnostics["executor_initial_sl"] == entry_diagnostics["executor_initial_sl"]
    runner.signal_store.close()


def test_breakeven_preserves_entry_snapshot_and_adds_breakeven_time(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    entry_row = runner.signal_store.get_executor_outcome(key)
    assert entry_row is not None
    entry_diagnostics = json.loads(entry_row["diagnostics_json"])

    signal.meta["executor_snapshot"] = make_snapshot(price=100.6, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.1)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "MOVE_SL_TO_BREAKEVEN"
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["executor_entry_time"] == entry_diagnostics["executor_entry_time"]
    assert diagnostics["executor_initial_sl"] == entry_diagnostics["executor_initial_sl"]
    assert diagnostics["breakeven_time"]
    runner.signal_store.close()


def test_exit_uses_lifecycle_enter_fallback_when_diagnostics_snapshot_missing(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    fallback_entry_time = "2026-05-01T00:00:00+00:00"
    runner.signal_store.add_trade_lifecycle_event(
        {
            "signal_key": key,
            "symbol": signal.symbol,
            "timeframe": "5",
            "side": "Buy",
            "event_type": "EXECUTOR_ENTER",
            "status": "ENTERED",
            "action": "ENTER_LONG",
            "reason": "entry_allowed_long",
            "price": 100.0,
            "created_at": fallback_entry_time,
        }
    )
    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET diagnostics_json = ? WHERE signal_key = ?",
        (json.dumps({}), key),
    )
    runner.signal_store.conn.commit()

    signal.meta["executor_snapshot"] = make_snapshot(price=100.1, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["entry_time"] == fallback_entry_time
    assert trades[0]["entry_price"] == 100.0
    assert trades[0]["initial_sl"] == 99.0
    runner.signal_store.close()


def test_exit_ledger_uses_executor_entry_time_not_outcome_created_at(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    entry_row = runner.signal_store.get_executor_outcome(key)
    assert entry_row is not None
    executor_entry_time = json.loads(entry_row["diagnostics_json"])["executor_entry_time"]
    stale_signal_time = "2020-01-01T00:00:00+00:00"
    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET created_at = ? WHERE signal_key = ?",
        (stale_signal_time, key),
    )
    runner.signal_store.conn.commit()

    signal.meta["executor_snapshot"] = make_snapshot(price=100.1, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["entry_time"] == executor_entry_time
    assert trades[0]["entry_time"] != stale_signal_time
    runner.signal_store.close()


def test_invalid_long_executor_initial_sl_does_not_inflate_r_result(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    diagnostics = json.loads(row["diagnostics_json"])
    diagnostics["executor_initial_sl"] = 101.0
    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET diagnostics_json = ? WHERE signal_key = ?",
        (json.dumps(diagnostics), key),
    )
    runner.signal_store.conn.commit()

    signal.meta["executor_snapshot"] = make_snapshot(price=100.1, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["initial_sl"] == 101.0
    assert trades[0]["r_result"] is None
    trade_diagnostics = json.loads(trades[0]["diagnostics_json"])
    assert trade_diagnostics["invalid_initial_sl"] is True
    runner.signal_store.close()


def test_refresh_open_position_writes_hold_without_new_signal_and_no_duplicate_enter(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    runner.trade_learning = TradeLearningEngine(runner.signal_store, logger=runner.logger)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET updated_at = ? WHERE signal_key = ?",
        ("2026-01-01T00:00:00+00:00", key),
    )
    runner.signal_store.conn.commit()

    refreshed = asyncio.run(runner.refresh_open_executor_positions())

    assert refreshed == 1
    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "HOLD"
    assert row["state"] == "ENTERED"
    assert row["updated_at"] > "2026-01-01T00:00:00+00:00"
    assert row_count(runner.signal_store) == 1
    enter_events = [
        event
        for event in runner.signal_store.get_trade_lifecycle_events(key)
        if event["event_type"] == "EXECUTOR_ENTER" or event["action"] == "ENTER_LONG"
    ]
    assert len(enter_events) == 1
    runner.signal_store.close()


def test_refresh_open_position_can_exit_and_write_trade_without_new_signal(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    runner.signal_store.conn.execute(
        """
        UPDATE executor_outcomes
        SET price = ?, buy_flow = ?, sell_flow = ?, volume_impulse = ?, updated_at = ?
        WHERE signal_key = ?
        """,
        (100.1, 80.0, 130.0, 1.0, "2026-01-01T00:00:00+00:00", key),
    )
    runner.signal_store.conn.commit()

    refreshed = asyncio.run(runner.refresh_open_executor_positions())

    assert refreshed == 1
    row = runner.signal_store.get_executor_outcome(key)
    assert row is not None
    assert row["action"] == "EXIT"
    assert row["state"] == "EXITED"
    assert row["exit_reason"] == "exit_sell_flow_dominance"
    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["signal_key"] == key
    runner.signal_store.close()


def test_invalid_short_initial_sl_does_not_calculate_r_result() -> None:
    assert (
        AccumulationRunner._executor_r_result(
            side="Sell",
            entry_price=100.0,
            exit_price=99.0,
            initial_sl=99.5,
            current_sl=99.5,
        )
        is None
    )


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


def test_management_v2_only_runs_in_paper_mode(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="live")
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    assert runner.trade_executor is None
    assert row_count(runner.signal_store) == 0
    runner.signal_store.close()


def test_run_accumulation_bat_contains_executor_defaults() -> None:
    text = Path("run_accumulation_v1.bat").read_text(encoding="utf-8")

    assert 'set "RUN_TRADE_EXECUTOR=true"' in text
    assert 'set "TRADE_EXECUTOR_MODE=paper"' in text
    assert 'set "EXECUTOR_MANAGEMENT_POLICY=trailing_40pct_giveback_after_1r"' in text
    assert 'set "EXECUTOR_PROTECT_AFTER_1R=true"' in text
    assert 'set "EXECUTOR_MIN_PROTECTED_R_AFTER_1R=0.25"' in text
    assert "echo RUN_TRADE_EXECUTOR=%RUN_TRADE_EXECUTOR%" in text
    assert "echo TRADE_EXECUTOR_MODE=%TRADE_EXECUTOR_MODE%" in text
    assert "echo EXECUTOR_MANAGEMENT_POLICY=%EXECUTOR_MANAGEMENT_POLICY%" in text
    assert "echo EXECUTOR_PROTECT_AFTER_1R=%EXECUTOR_PROTECT_AFTER_1R%" in text
    assert "echo EXECUTOR_MIN_PROTECTED_R_AFTER_1R=%EXECUTOR_MIN_PROTECTED_R_AFTER_1R%" in text


def test_new_executor_entry_diagnostics_do_not_preserve_stale_breakeven_time(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    previous_diagnostics = {
        "executor_entry_time": "2026-06-02T15:00:00+00:00",
        "executor_entry_price": 1.0,
        "executor_initial_sl": 0.95,
        "executor_side": "Buy",
        "executor_signal_key": "XLMUSDT|linear|5|PRE_IMPULSE_ZONE|Buy",
        "executor_timeframe": "5",
        "breakeven_time": "2026-06-02T15:08:53+00:00",
    }
    previous_row = runner.signal_store.upsert_executor_decision(
        signal_key="XLMUSDT|linear|5|PRE_IMPULSE_ZONE|Buy",
        symbol="XLMUSDT",
        side="Buy",
        state="EXITED",
        action="EXIT",
        reason="exit_stop_loss_hit",
        entry_price=1.0,
        current_sl=1.0,
        exit_price=1.0,
        max_gain_r=1.2,
        max_drawdown_r=-0.1,
        bars_in_trade=4,
        diagnostics_json=previous_diagnostics,
    )
    fresh_diagnostics: dict[str, object] = {}

    runner._preserve_executor_entry_diagnostics(fresh_diagnostics, previous_row, preserve_breakeven_time=False)

    assert fresh_diagnostics["executor_entry_time"] == previous_diagnostics["executor_entry_time"]
    assert fresh_diagnostics["executor_entry_price"] == previous_diagnostics["executor_entry_price"]
    assert fresh_diagnostics["executor_initial_sl"] == previous_diagnostics["executor_initial_sl"]
    assert "breakeven_time" not in fresh_diagnostics
    runner.signal_store.close()


def test_absorption_blocked_entry_is_stored_with_gate_diagnostics(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        kind="ABSORPTION_ZONE",
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_NEUTRAL",
            "market_regime": "RISK_ON",
            "executor_snapshot": make_snapshot(buy_flow=110.0, sell_flow=100.0, volume_impulse=1.4),
        },
    )

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert row is not None
    assert row["action"] == "WATCH"
    assert row["reason"] == "entry_blocked_absorption_weak_confirmation"
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["absorption_strict_gate"] is True
    assert diagnostics["absorption_gate_passed"] is False
    assert diagnostics["absorption_gate_reason"] == "entry_blocked_absorption_weak_confirmation"
    assert diagnostics["btc_regime"] == "BTC_NEUTRAL"
    assert diagnostics["market_regime"] == "RISK_ON"
    assert diagnostics["buy_flow"] == 110.0
    assert diagnostics["sell_flow"] == 100.0
    assert diagnostics["volume_impulse"] == 1.4
    assert diagnostics["required_volume_impulse"] == 1.2
    assert diagnostics["spread_bps"] == 4.0
    assert diagnostics["ask_wall_strength"] == 0.2
    assert "support" in diagnostics
    assert "resistance" in diagnostics
    runner.signal_store.close()

class _FakeTestnetOrderExecutor:
    def __init__(self, result: dict | None = None):
        self.result = result or {"ok": True, "status": "placed", "order_id": "tn-1", "qty": 1.0, "notional_usdt": 100.0}
        self.entry_calls = 0
        self.exit_calls = 0
        self.exit_payloads = []

    def place_entry_order(self, **kwargs):
        self.entry_calls += 1
        return dict(self.result)

    def place_exit_order(self, **kwargs):
        self.exit_calls += 1
        self.exit_payloads.append(kwargs)
        return {"ok": True, "status": "placed", "order_id": "exit-1", "qty": 1.0, "notional_usdt": 101.0}


def make_testnet_runner(tmp_path: Path, fake_executor: _FakeTestnetOrderExecutor) -> AccumulationRunner:
    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.settings = DummySettings()
    runner.logger = logging.getLogger("test.testnet_executor")
    runner.signal_store = SignalStore(db_path=str(tmp_path / "signals.db"))
    runner.trade_executor_mode = "testnet"
    runner.trade_executor_enabled = True
    runner.trade_executor = SmartTradeExecutor(trade_executor_mode="testnet")
    runner.testnet_order_executor = fake_executor
    runner.executor_exit_shadow_enabled = False
    runner.trade_learning = None
    return runner


def test_testnet_mode_places_order_and_records_diagnostics(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 1
    assert row["action"] == "ENTER_LONG"
    assert diagnostics["trade_executor_mode"] == "testnet"
    assert diagnostics["testnet_order_attempted"] is True
    assert diagnostics["testnet_order_status"] == "placed"
    assert diagnostics["testnet_order_id"] == "tn-1"
    runner.signal_store.close()


def test_non_testnet_mode_does_not_call_testnet_order_executor(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    fake = _FakeTestnetOrderExecutor()
    runner.testnet_order_executor = fake
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    assert fake.entry_calls == 0
    runner.signal_store.close()


def test_testnet_entry_block_keeps_watch_state(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor({"ok": False, "status": "blocked", "reason": "entry_blocked_insufficient_testnet_balance"})
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert row["action"] == "WATCH"
    assert row["state"] == "TRADE_WATCH"
    assert row["reason"] == "entry_blocked_insufficient_testnet_balance"
    assert diagnostics["testnet_order_status"] == "blocked"
    assert diagnostics["testnet_blocked_reason"] == "entry_blocked_insufficient_testnet_balance"
    runner.signal_store.close()


def test_testnet_exit_uses_reduce_only_executor_path(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=98.8, buy_flow=50.0, sell_flow=200.0, volume_impulse=1.6)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert row["action"] == "EXIT"
    assert fake.exit_calls == 1
    assert fake.exit_payloads[0]["signal_key"] == key
    assert diagnostics["testnet_order_status"] == "placed"
    assert diagnostics["testnet_order_id"] == "exit-1"
    runner.signal_store.close()


def test_testnet_risk_off_exception_records_diagnostics_json(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        kind="PRE_IMPULSE_ZONE",
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_BULLISH",
            "market_regime": "RISK-OFF",
            "executor_snapshot": make_snapshot(buy_flow=106.0, sell_flow=100.0, volume_impulse=0.90),
        },
    )
    key = signal_key(signal)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 1
    assert row["action"] == "ENTER_LONG"
    assert diagnostics["trade_executor_mode"] == "testnet"
    assert diagnostics["testnet_risk_off_exception"] is True
    assert diagnostics["testnet_entry_gate_relaxed"] is True
    assert diagnostics["testnet_relaxation_reason"] == "strong_testnet_entry_during_risk_off"
    assert diagnostics["signal_kind"] == "PRE_IMPULSE_ZONE"
    assert diagnostics["btc_regime"] == "BTC_BULLISH"
    assert diagnostics["market_regime"] == "RISK-OFF"
    assert diagnostics["buy_flow"] == 106.0
    assert diagnostics["sell_flow"] == 100.0
    assert diagnostics["volume_impulse"] == 0.90
    assert diagnostics["required_volume_impulse"] == 1.2
    assert diagnostics["ask_wall_strength"] == 0.2
    assert diagnostics["spread_bps"] == 4.0
    runner.signal_store.close()
