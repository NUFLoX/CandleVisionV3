from __future__ import annotations

import asyncio
import json
import logging
import math
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
from orderflow_accum.trade_executor import (
    MANAGEMENT_POLICY_LEGACY,
    MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R,
    SmartTradeExecutor,
)
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



def _executor_builder_runner(mode: str, settings: object | None = None) -> AccumulationRunner:
    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.settings = settings or DummySettings()
    runner.trade_executor_mode = mode
    return runner


def test_build_trade_executor_testnet_uses_configured_management_policy(monkeypatch) -> None:
    monkeypatch.setenv("EXECUTOR_MANAGEMENT_POLICY", MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R)
    monkeypatch.setenv("EXECUTOR_PROTECT_AFTER_1R", "true")
    monkeypatch.setenv("EXECUTOR_MIN_PROTECTED_R_AFTER_1R", "0.5")

    executor = _executor_builder_runner("testnet")._build_trade_executor()

    assert executor.trade_executor_mode == "testnet"
    assert executor.management_policy == MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R
    assert executor.protect_after_1r is True
    assert executor.min_protected_r_after_1r == 0.5


def test_build_trade_executor_paper_uses_configured_management_policy(monkeypatch) -> None:
    monkeypatch.setenv("EXECUTOR_MANAGEMENT_POLICY", MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R)

    executor = _executor_builder_runner("paper")._build_trade_executor()

    assert executor.trade_executor_mode == "paper"
    assert executor.management_policy == MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R


def test_build_trade_executor_defaults_to_legacy_management_policy(monkeypatch) -> None:
    monkeypatch.delenv("EXECUTOR_MANAGEMENT_POLICY", raising=False)
    monkeypatch.delenv("EXECUTOR_PROTECT_AFTER_1R", raising=False)
    monkeypatch.delenv("EXECUTOR_MIN_PROTECTED_R_AFTER_1R", raising=False)

    paper_executor = _executor_builder_runner("paper")._build_trade_executor()
    testnet_executor = _executor_builder_runner("testnet")._build_trade_executor()

    assert paper_executor.management_policy == MANAGEMENT_POLICY_LEGACY
    assert testnet_executor.management_policy == MANAGEMENT_POLICY_LEGACY
    assert paper_executor.protect_after_1r is False
    assert testnet_executor.protect_after_1r is False
    assert paper_executor.min_protected_r_after_1r == 0.25
    assert testnet_executor.min_protected_r_after_1r == 0.25


def test_build_trade_executor_settings_management_policy_overrides_env(monkeypatch) -> None:
    monkeypatch.setenv("EXECUTOR_MANAGEMENT_POLICY", MANAGEMENT_POLICY_LEGACY)
    settings = type(
        "ConfiguredSettings",
        (),
        {
            "market_categories": ["linear"],
            "executor_management_policy": MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R,
        },
    )()

    executor = _executor_builder_runner("testnet", settings=settings)._build_trade_executor()

    assert executor.management_policy == MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R


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


def test_stop_loss_exit_creates_trade_diagnosis(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        kind="CONFIRMED_LONG",
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_BULLISH",
            "market_regime": "RISK_ON",
            "executor_snapshot": make_snapshot(),
        },
    )
    key = signal_key(signal)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=95.0, buy_flow=140.0, sell_flow=90.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    diagnosis = runner.signal_store.get_stop_loss_diagnosis(trades[0]["trade_key"])
    assert diagnosis is not None
    assert diagnosis["diagnosis_type"] == "STOP_LOSS"
    assert diagnosis["trade_key"] == trades[0]["trade_key"]
    assert diagnosis["signal_key"] == key
    assert diagnosis["entry_price"] == 100.0
    assert diagnosis["initial_sl"] == 99.0
    assert diagnosis["exit_price"] == 99.0
    assert round(float(diagnosis["r_result"]), 6) == -1.0
    assert diagnosis["signal_kind"] == "CONFIRMED_LONG"
    assert diagnosis["btc_regime"] == "BTC_BULLISH"
    assert diagnosis["market_regime"] == "RISK_ON"
    assert diagnosis["post_stop_observation_pending"] is True
    assert diagnosis["post_stop_check_after_bars"] == [3, 6, 12, 24]
    assert diagnosis["features"]["observed_exit_price"] == 95.0
    runner.signal_store.close()


def test_non_stop_exit_does_not_create_stop_loss_diagnosis(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    signal.meta["executor_snapshot"] = make_snapshot(price=100.1, buy_flow=80.0, sell_flow=130.0, volume_impulse=1.0)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    assert trades[0]["exit_reason"] == "exit_sell_flow_dominance"
    assert runner.signal_store.get_stop_loss_diagnosis(trades[0]["trade_key"]) is None
    assert runner.signal_store.stop_loss_diagnosis_summary()["stop_loss_count"] == 0
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
    assert diagnostics["executor_management_policy"] == MANAGEMENT_POLICY_LEGACY
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


def test_testnet_processes_strong_pre_impulse_without_confirmed_long(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        kind="PRE_IMPULSE_ZONE",
        score=8.0,
        reasons=["PRE_IMPULSE_ZONE"],
        meta={"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL", "executor_snapshot": make_snapshot()},
    )

    runner._process_paper_executor(signal, "linear", "PRE_IMPULSE")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 1
    assert row["action"] == "ENTER_LONG"
    assert diagnostics["testnet_observation_entry_candidate"] is True
    assert diagnostics["testnet_observation_entry_reason"] == "strong_non_confirmed_buy_signal"
    assert diagnostics["original_signal_status"] == "PRE_IMPULSE"
    assert diagnostics["original_signal_kind"] == "PRE_IMPULSE_ZONE"
    assert diagnostics["original_signal_score"] == 8.0
    runner.signal_store.close()


def test_testnet_processes_strong_breakout_pressure_without_confirmed_long(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        kind="BREAKOUT_PRESSURE",
        score=8.5,
        reasons=["BREAKOUT_PRESSURE"],
        meta={"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL", "executor_snapshot": make_snapshot()},
    )

    runner._process_paper_executor(signal, "linear", "BREAKOUT_PRESSURE")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 1
    assert row["action"] == "ENTER_LONG"
    assert diagnostics["testnet_observation_entry_candidate"] is True
    assert diagnostics["original_signal_status"] == "BREAKOUT_PRESSURE"
    assert diagnostics["original_signal_kind"] == "BREAKOUT_PRESSURE"
    runner.signal_store.close()


def test_paper_mode_still_ignores_non_confirmed_observation_signal(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    signal = make_signal(
        kind="PRE_IMPULSE_ZONE",
        score=9.0,
        reasons=["PRE_IMPULSE_ZONE"],
        meta={"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL", "executor_snapshot": make_snapshot()},
    )

    runner._process_paper_executor(signal, "linear", "PRE_IMPULSE")

    assert row_count(runner.signal_store) == 0
    runner.signal_store.close()


def test_testnet_observation_weak_score_is_ignored(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        kind="BASE_BUILDUP_LONG",
        score=7.99,
        reasons=["BASE_BUILDUP_LONG"],
        meta={"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL", "executor_snapshot": make_snapshot()},
    )

    runner._process_paper_executor(signal, "linear", "WATCHING")

    assert fake.entry_calls == 0
    assert row_count(runner.signal_store) == 0
    runner.signal_store.close()


def test_testnet_observation_bearish_or_dump_btc_is_ignored(tmp_path: Path) -> None:
    for btc_regime in ("BTC_BEARISH", "BTC_DUMP_RISK"):
        fake = _FakeTestnetOrderExecutor()
        regime_path = tmp_path / btc_regime
        regime_path.mkdir()
        runner = make_testnet_runner(regime_path, fake)
        signal = make_signal(
            kind="ACCUMULATION_LONG_READY",
            score=9.0,
            reasons=["ACCUMULATION_LONG_READY"],
            meta={"tf": "5", "market": "linear", "btc_regime": btc_regime, "executor_snapshot": make_snapshot()},
        )

        runner._process_paper_executor(signal, "linear", "ACCUMULATION")

        assert fake.entry_calls == 0
        assert row_count(runner.signal_store) == 0
        runner.signal_store.close()


def test_testnet_observation_enter_long_uses_existing_order_path(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        kind="ACCUMULATION_LONG_READY",
        score=8.0,
        reasons=["ACCUMULATION_LONG_READY"],
        meta={"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL", "executor_snapshot": make_snapshot()},
    )

    runner._process_paper_executor(signal, "linear", "PENDING")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 1
    assert row["action"] == "ENTER_LONG"
    assert diagnostics["testnet_order_attempted"] is True
    assert diagnostics["testnet_order_status"] == "placed"
    assert diagnostics["testnet_observation_entry_candidate"] is True
    runner.signal_store.close()


def test_testnet_observation_watch_persists_diagnostics_without_order(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        kind="PRE_IMPULSE_ZONE",
        score=8.0,
        reasons=["PRE_IMPULSE_ZONE"],
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_NEUTRAL",
            "executor_snapshot": make_snapshot(buy_flow=90.0, sell_flow=100.0, volume_impulse=1.4),
        },
    )

    runner._process_paper_executor(signal, "linear", "PRE_IMPULSE")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 0
    assert row["action"] == "WATCH"
    assert row["state"] == "TRADE_WATCH"
    assert row["updated_at"]
    assert diagnostics["testnet_order_status"] == "not_attempted"
    assert diagnostics["testnet_observation_entry_candidate"] is True
    assert diagnostics["testnet_observation_entry_reason"] == "strong_non_confirmed_buy_signal"
    assert diagnostics["original_signal_status"] == "PRE_IMPULSE"
    assert diagnostics["original_signal_kind"] == "PRE_IMPULSE_ZONE"
    assert diagnostics["original_signal_score"] == 8.0
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


def test_percent_move_is_normalized_before_active_r_conversion(tmp_path: Path) -> None:
    entry = 1.13365
    initial_risk = entry - 1.1141475

    active_r = AccumulationRunner._active_r_from_fractional_price_move(
        entry_price=entry,
        initial_risk=initial_risk,
        move=2.26,
    )

    assert math.isclose(active_r, (entry * 0.0226) / initial_risk, rel_tol=0.0, abs_tol=1e-12)
    assert 1.30 < active_r < 1.32


def test_active_buy_suspicious_r_recovers_from_current_price(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        symbol="ENAUSDT",
        entry=1.13365,
        stop_loss=1.1141475,
        meta={"tf": "5", "market": "linear"},
    )
    key = signal_key(signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=131.0,
        max_drawdown_r=0.1,
        bars_in_trade=7,
        price=1.1592,
        diagnostics_json={
            "executor_entry_price": 1.13365,
            "executor_initial_sl": 1.1141475,
        },
    )

    position = runner._position_from_executor_row(signal, runner.signal_store.get_executor_outcome(key))
    diagnostics = json.loads(runner.signal_store.get_executor_outcome(key)["diagnostics_json"])

    assert 1.30 < position.max_gain_r < 1.32
    assert position.max_gain_r < 10.0
    assert diagnostics["suspicious_active_r_scale"] is True
    assert diagnostics["active_r_recovered_from"] == "current_price"
    assert diagnostics["active_r_scale_original_max_gain_r"] == 131.0
    runner.signal_store.close()


def test_active_buy_recovery_uses_initial_sl_not_trailing_current_sl(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        symbol="ENAUSDT",
        entry=1.13365,
        stop_loss=1.1141475,
        meta={"tf": "5", "market": "linear"},
    )
    key = signal_key(signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=2.0,
        max_drawdown_r=0.1,
        bars_in_trade=7,
        diagnostics_json={"executor_entry_price": 1.13365, "executor_initial_sl": 1.1141475},
    )

    row = runner.signal_store.get_executor_outcome(key)
    position = runner._position_from_executor_row(signal, row)
    diagnostics = json.loads(runner.signal_store.get_executor_outcome(key)["diagnostics_json"])

    assert position.current_sl == 1.1379025
    assert position.stop_loss == 1.1141475
    assert abs(position.initial_risk - (1.13365 - 1.1141475)) < 1e-12
    assert position.max_gain_r == 2.0
    assert diagnostics["risk_basis"] == "initial_sl"
    assert diagnostics["initial_risk"] == position.initial_risk
    assert diagnostics["invalid_initial_risk"] is False
    assert "risk_basis_warning" not in diagnostics
    runner.signal_store.close()


def test_active_buy_suspicious_r_without_current_price_resets_and_does_not_use_current_sl(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        symbol="ENAUSDT",
        entry=1.13365,
        stop_loss=1.1141475,
        meta={"tf": "5", "market": "linear"},
    )
    key = signal_key(signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=131.0,
        max_drawdown_r=0.1,
        bars_in_trade=7,
        diagnostics_json={"executor_entry_price": 1.13365},
    )

    row = runner.signal_store.get_executor_outcome(key)
    position = runner._position_from_executor_row(signal, row)
    updated = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(updated["diagnostics_json"])

    assert position.max_gain_r == 0.0
    assert updated["max_gain_r"] == 0.0
    assert diagnostics["risk_basis"] == "initial_sl"
    assert diagnostics["risk_source"] == "fallback_signal_stop_loss"
    assert diagnostics["risk_basis_warning"] == "missing_executor_initial_sl"
    assert diagnostics["invalid_initial_risk"] is False
    assert diagnostics["initial_risk"] == position.initial_risk
    assert diagnostics["suspicious_active_r_scale"] is True
    assert diagnostics["active_r_recovered_from"] == "reset"
    runner.signal_store.close()


def test_active_buy_valid_r_remains_unchanged(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        symbol="ENAUSDT",
        entry=1.13365,
        stop_loss=1.1141475,
        meta={"tf": "5", "market": "linear"},
    )
    key = signal_key(signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=1.3,
        max_drawdown_r=0.1,
        bars_in_trade=7,
        price=1.1592,
        diagnostics_json={"executor_entry_price": 1.13365, "executor_initial_sl": 1.1141475},
    )

    position = runner._position_from_executor_row(signal, runner.signal_store.get_executor_outcome(key))
    diagnostics = json.loads(runner.signal_store.get_executor_outcome(key)["diagnostics_json"])

    assert position.max_gain_r == 1.3
    assert runner.signal_store.get_executor_outcome(key)["max_gain_r"] == 1.3
    assert diagnostics["suspicious_active_r_scale"] is False
    runner.signal_store.close()


def test_active_recovery_malformed_diagnostics_resets_suspicious_r_safely(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        symbol="ENAUSDT",
        entry=1.13365,
        stop_loss=1.1141475,
        meta={"tf": "5", "market": "linear"},
    )
    key = signal_key(signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=131.0,
        max_drawdown_r=0.1,
        bars_in_trade=7,
        diagnostics_json="{not-json",
    )

    position = runner._position_from_executor_row(signal, runner.signal_store.get_executor_outcome(key))
    diagnostics = json.loads(runner.signal_store.get_executor_outcome(key)["diagnostics_json"])

    assert position.max_gain_r == 0.0
    assert diagnostics["suspicious_active_r_scale"] is True
    assert diagnostics["active_r_recovered_from"] == "reset"
    runner.signal_store.close()


def test_active_recovery_does_not_modify_closed_executor_trade_rows(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(
        symbol="ENAUSDT",
        entry=1.13365,
        stop_loss=1.1141475,
        meta={"tf": "5", "market": "linear"},
    )
    key = signal_key(signal)
    trade_key = f"{key}|closed"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=131.0,
        max_drawdown_r=0.1,
        bars_in_trade=7,
        diagnostics_json={"executor_initial_sl": 1.1141475},
    )
    runner.signal_store.upsert_executor_trade(
        {
            "trade_key": trade_key,
            "signal_key": key,
            "symbol": "ENAUSDT",
            "timeframe": "5",
            "side": "Buy",
            "state": "EXITED",
            "entry_price": 1.13365,
            "exit_price": 1.15,
            "initial_sl": 1.1141475,
            "final_sl": 1.1379025,
            "current_sl": 1.1379025,
            "exit_time": "2026-06-04T10:00:00+00:00",
            "max_gain_r": 131.0,
            "max_drawdown_r": 0.2,
        }
    )

    before = runner.signal_store.get_executor_trade(trade_key)
    runner._position_from_executor_row(signal, runner.signal_store.get_executor_outcome(key))
    after = runner.signal_store.get_executor_trade(trade_key)

    assert after == before
    assert after["max_gain_r"] == 131.0
    runner.signal_store.close()

def test_active_recovery_prefers_executor_trade_initial_sl_before_current_sl(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(symbol="ENAUSDT", entry=1.13365, meta={"tf": "5", "market": "linear"})
    key = signal_key(signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="ENAUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=1.13365,
        current_sl=1.1379025,
        max_gain_r=1.5,
        max_drawdown_r=0.2,
        bars_in_trade=4,
        diagnostics_json={"executor_entry_price": 1.13365},
    )
    runner.signal_store.upsert_executor_trade(
        {
            "trade_key": f"{key}|closed",
            "signal_key": key,
            "symbol": "ENAUSDT",
            "timeframe": "5",
            "side": "Buy",
            "state": "EXITED",
            "entry_price": 1.13365,
            "exit_price": 1.15,
            "initial_sl": 1.1141475,
            "final_sl": 1.1379025,
            "current_sl": 1.1379025,
            "exit_time": "2026-06-04T10:00:00+00:00",
            "max_gain_r": 1.5,
            "max_drawdown_r": 0.2,
        }
    )

    position = runner._position_from_executor_row(signal, runner.signal_store.get_executor_outcome(key))
    diagnostics = json.loads(runner.signal_store.get_executor_outcome(key)["diagnostics_json"])

    assert position.stop_loss == 1.1141475
    assert diagnostics["risk_source"] == "executor_trades.initial_sl"
    assert diagnostics["invalid_initial_risk"] is False
    assert abs(diagnostics["initial_risk"] - (1.13365 - 1.1141475)) < 1e-12
    runner.signal_store.close()


def _hybrid_row(runner: AccumulationRunner, signal: Signal, scenario: str) -> dict:
    row = runner.signal_store.get_hybrid_entry_shadow(signal_key(signal), scenario)
    assert row is not None
    return row


def test_hybrid_shadow_observation_created_without_changing_executor_decision(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot(buy_flow=100.0, sell_flow=100.0)})
    key = signal_key(signal)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    outcome = runner.signal_store.get_executor_outcome(key)
    assert outcome["action"] == "WATCH"
    assert _hybrid_row(runner, signal, "pullback_shadow")["status"] in {"OBSERVING", "ENTERED"}
    assert _hybrid_row(runner, signal, "momentum_0_5r_shadow")["status"] == "OBSERVING"
    runner.signal_store.close()


def test_hybrid_momentum_half_r_creates_shadow_entry_when_orderflow_confirms(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    signal = make_signal(
        stop_loss=98.0,
        meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot(price=101.0, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.5)},
    )

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = _hybrid_row(runner, signal, "momentum_0_5r_shadow")
    features = json.loads(row["features_json"])
    assert row["status"] == "ENTERED"
    assert row["shadow_entry_price"] == 101.0
    assert features["momentum_triggered_at_r"] == 0.5
    runner.signal_store.close()


def test_hybrid_momentum_does_not_enter_in_btc_bearish_or_dump_risk(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    signal = make_signal(
        stop_loss=98.0,
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_DUMP_RISK",
            "executor_snapshot": make_snapshot(price=101.0, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.5),
        },
    )

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = _hybrid_row(runner, signal, "momentum_0_5r_shadow")
    features = json.loads(row["features_json"])
    assert row["status"] == "OBSERVING"
    assert features["btc_regime_ok"] is False
    runner.signal_store.close()


def test_hybrid_momentum_does_not_chase_after_one_r(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    signal = make_signal(
        stop_loss=98.0,
        meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot(price=102.1, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.5)},
    )

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = _hybrid_row(runner, signal, "momentum_0_5r_shadow")
    features = json.loads(row["features_json"])
    assert row["status"] == "MISSED"
    assert row["reason"] == "missed_momentum_too_late"
    assert features["missed_momentum_too_late"] is True
    runner.signal_store.close()


def test_hybrid_pullback_can_create_shadow_entry_after_retest_holds(tmp_path: Path) -> None:
    runner = make_runner(tmp_path, enabled=True, mode="paper")
    signal = make_signal(
        stop_loss=98.0,
        meta={
            "tf": "5",
            "market": "linear",
            "executor_snapshot": make_snapshot(price=99.8, buy_flow=110.0, sell_flow=100.0, support=99.5, vwap=99.75),
        },
    )

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = _hybrid_row(runner, signal, "pullback_shadow")
    features = json.loads(row["features_json"])
    assert row["status"] == "ENTERED"
    assert row["reason"] == "pullback_retest_held_orderflow_recovered"
    assert features["pullback_depth_r"] > 0
    assert features["support_holds"] is True
    runner.signal_store.close()


def test_hybrid_shadow_does_not_place_bybit_testnet_order_when_actual_entry_blocked(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        stop_loss=98.0,
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_DUMP_RISK",
            "executor_snapshot": make_snapshot(price=101.0, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.5),
        },
    )

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    assert fake.entry_calls == 0
    assert _hybrid_row(runner, signal, "momentum_0_5r_shadow")["status"] == "OBSERVING"
    runner.signal_store.close()


def test_active_executor_outcome_rejects_doge_style_polluted_price(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    key = "DOGEUSDT|linear|5|CONFIRMED_LONG|Buy"

    row = runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=0.085505,
        current_sl=0.0869886075,
        max_gain_r=52.99,
        max_drawdown_r=0.0,
        bars_in_trade=12,
        price=0.4,
        diagnostics_json={
            "executor_entry_price": 0.085505,
            "executor_initial_sl": 0.07957057,
            "initial_risk": 0.00593443,
        },
    )
    diagnostics = json.loads(row["diagnostics_json"])

    assert row["price"] == 0.085505
    assert row["max_gain_r"] <= 25.0
    assert row["max_gain_r"] == 0.0
    assert diagnostics["suspicious_active_price"] is True
    assert diagnostics["active_price_rejected"] is True
    assert diagnostics["active_price_rejected_value"] == 0.4
    assert diagnostics["active_price_recovery_source"] == "entry_price"
    runner.signal_store.close()


def test_active_executor_outcome_rejects_polluted_previous_price(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    key = "DOGEUSDT|linear|5|CONFIRMED_LONG|Buy"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=0.085505,
        current_sl=0.0869886075,
        max_gain_r=0.0,
        max_drawdown_r=0.0,
        bars_in_trade=11,
        price=0.085505,
        diagnostics_json={
            "executor_entry_price": 0.085505,
            "executor_initial_sl": 0.07957057,
            "initial_risk": 0.00593443,
        },
    )
    runner.signal_store.conn.execute(
        """
        UPDATE executor_outcomes
        SET price = ?, max_gain_r = ?, diagnostics_json = ?
        WHERE signal_key = ?
        """,
        (
            0.4,
            52.99,
            json.dumps(
                {
                    "executor_entry_price": 0.085505,
                    "executor_initial_sl": 0.07957057,
                    "initial_risk": 0.00593443,
                    "suspicious_active_price": True,
                    "active_price_rejected": True,
                    "active_price_rejected_value": 0.4,
                    "active_price_recovery_source": "previous_price",
                }
            ),
            key,
        ),
    )
    runner.signal_store.conn.commit()

    row = runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=0.085505,
        current_sl=0.0869886075,
        max_gain_r=52.99,
        max_drawdown_r=0.0,
        bars_in_trade=12,
        price=0.4,
        diagnostics_json={
            "executor_entry_price": 0.085505,
            "executor_initial_sl": 0.07957057,
            "initial_risk": 0.00593443,
        },
    )
    diagnostics = json.loads(row["diagnostics_json"])

    assert row["price"] == 0.085505
    assert row["max_gain_r"] == 0.0
    assert row["max_gain_r"] <= 25.0
    assert diagnostics["suspicious_active_price"] is True
    assert diagnostics["active_price_rejected"] is True
    assert diagnostics["active_price_rejected_value"] == 0.4
    assert diagnostics["previous_active_price_rejected"] is True
    assert diagnostics["previous_active_price_rejected_value"] == 0.4
    assert diagnostics["active_price_recovery_source"] == "entry_price"
    assert diagnostics["suspicious_active_r_reset"] is True
    runner.signal_store.close()


def test_active_executor_outcome_rejected_price_uses_previous_valid_price_and_r(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    key = "DOGEUSDT|linear|5|CONFIRMED_LONG|Buy"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="ENTERED",
        action="ENTER_LONG",
        reason="entry_allowed_long",
        entry_price=0.085505,
        current_sl=0.07957057,
        max_gain_r=0.0,
        max_drawdown_r=0.0,
        bars_in_trade=0,
        price=0.086,
        diagnostics_json={"executor_initial_sl": 0.07957057},
    )

    row = runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=0.085505,
        current_sl=0.0869886075,
        max_gain_r=52.99,
        max_drawdown_r=0.0,
        bars_in_trade=12,
        price=0.4,
        diagnostics_json={"executor_initial_sl": 0.07957057},
    )
    diagnostics = json.loads(row["diagnostics_json"])

    assert row["price"] == 0.086
    assert 0.08 < row["max_gain_r"] < 0.09
    assert row["max_gain_r"] <= 25.0
    assert diagnostics["active_price_recovery_source"] == "previous_price"
    runner.signal_store.close()


def test_active_executor_outcome_accepts_valid_price_near_entry(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    key = "DOGEUSDT|linear|5|CONFIRMED_LONG|Buy"

    row = runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="TRAILING_PROFIT",
        action="HOLD",
        reason="hold_position",
        entry_price=0.085505,
        current_sl=0.07957057,
        max_gain_r=0.42,
        max_drawdown_r=0.0,
        bars_in_trade=3,
        price=0.088,
        diagnostics_json={"executor_initial_sl": 0.07957057},
    )
    diagnostics = json.loads(row["diagnostics_json"])

    assert row["price"] == 0.088
    assert row["max_gain_r"] == 0.42
    assert diagnostics["suspicious_active_price"] is False
    assert diagnostics["active_price_rejected"] is False
    runner.signal_store.close()


def test_closed_executor_outcome_rows_are_not_active_price_validated(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    key = "DOGEUSDT|linear|5|CONFIRMED_LONG|Buy"

    row = runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol="DOGEUSDT",
        side="Buy",
        state="EXITED",
        action="EXIT",
        reason="exit_stop_loss_hit",
        entry_price=0.085505,
        current_sl=0.0869886075,
        exit_price=0.4,
        exit_reason="exit_stop_loss_hit",
        max_gain_r=52.99,
        max_drawdown_r=0.0,
        bars_in_trade=12,
        price=0.4,
        diagnostics_json={"executor_initial_sl": 0.07957057},
    )
    diagnostics = json.loads(row["diagnostics_json"])

    assert row["state"] == "EXITED"
    assert row["price"] == 0.4
    assert row["max_gain_r"] == 52.99
    assert "active_price_rejected" not in diagnostics
    runner.signal_store.close()


def _force_executor_outcome_updated_at(runner: AccumulationRunner, signal_key_value: str, updated_at: str) -> None:
    runner.signal_store.conn.execute(
        "UPDATE executor_outcomes SET created_at = ?, updated_at = ? WHERE signal_key = ?",
        (updated_at, updated_at, signal_key_value),
    )
    runner.signal_store.conn.commit()


def test_fresh_signal_after_old_closed_outcome_creates_new_executor_attempt(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    old_updated_at = "2026-06-01T00:00:00+00:00"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol=signal.symbol,
        side=signal.side,
        state="CLOSED",
        action="WATCH",
        reason="old_closed_outcome",
        entry_price=signal.entry,
        current_sl=signal.stop_loss,
        diagnostics_json={"executor_entry_time": "2026-05-31T23:59:00+00:00"},
    )
    _force_executor_outcome_updated_at(runner, key, old_updated_at)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 1
    assert row["action"] == "ENTER_LONG"
    assert row["state"] == "ENTERED"
    assert row["reason"] == "entry_allowed_long"
    assert row["updated_at"] != old_updated_at
    assert diagnostics["previous_terminal_outcome_reused"] is False
    assert diagnostics["new_executor_attempt_after_terminal"] is True
    assert diagnostics["previous_terminal_state"] == "CLOSED"
    assert diagnostics["previous_terminal_reason"] == "old_closed_outcome"
    assert diagnostics["previous_terminal_updated_at"] == old_updated_at
    runner.signal_store.close()


def test_fresh_signal_after_old_exited_outcome_creates_new_executor_attempt(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)
    old_updated_at = "2026-06-02T00:00:00+00:00"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol=signal.symbol,
        side=signal.side,
        state="EXITED",
        action="EXIT",
        reason="take_profit",
        entry_price=signal.entry,
        current_sl=signal.stop_loss,
        exit_price=signal.take_profit_1,
        exit_reason="take_profit",
        diagnostics_json={},
    )
    _force_executor_outcome_updated_at(runner, key, old_updated_at)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert row["action"] == "ENTER_LONG"
    assert row["state"] == "ENTERED"
    assert row["updated_at"] != old_updated_at
    assert diagnostics["previous_terminal_outcome_reused"] is False
    assert diagnostics["new_executor_attempt_after_terminal"] is True
    assert diagnostics["previous_terminal_state"] == "EXITED"
    assert diagnostics["previous_terminal_reason"] == "take_profit"
    assert diagnostics["previous_terminal_updated_at"] == old_updated_at
    runner.signal_store.close()


def test_active_entered_executor_outcome_is_not_duplicated(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    key = signal_key(signal)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")
    first_id = runner.signal_store.get_executor_outcome(key)["id"]
    signal.meta["executor_snapshot"] = make_snapshot(price=100.4, buy_flow=150.0, sell_flow=90.0, volume_impulse=1.5)
    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert row_count(runner.signal_store) == 1
    assert row["id"] == first_id
    assert row["state"] in {"ENTERED", "PROTECT_BREAKEVEN", "TRAILING_PROFIT"}
    assert diagnostics["new_executor_attempt_after_terminal"] is False
    runner.signal_store.close()


def test_terminal_outcome_blocked_entry_becomes_trade_watch_with_fresh_timestamp(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    signal = make_signal(
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_DUMP_RISK",
            "executor_snapshot": make_snapshot(buy_flow=80.0, sell_flow=120.0, volume_impulse=0.6),
        }
    )
    key = signal_key(signal)
    old_updated_at = "2026-06-03T00:00:00+00:00"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol=signal.symbol,
        side=signal.side,
        state="TRADE_WATCH",
        action="PHANTOM_RESET",
        reason="old_phantom_reset",
        entry_price=signal.entry,
        current_sl=signal.stop_loss,
        diagnostics_json={},
    )
    _force_executor_outcome_updated_at(runner, key, old_updated_at)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert fake.entry_calls == 0
    assert row["action"] == "WATCH"
    assert row["state"] == "TRADE_WATCH"
    assert row["updated_at"] != old_updated_at
    assert diagnostics["previous_terminal_outcome_reused"] is False
    assert diagnostics["new_executor_attempt_after_terminal"] is True
    assert diagnostics["previous_terminal_state"] == "TRADE_WATCH"
    assert diagnostics["previous_terminal_reason"] == "old_phantom_reset"
    assert diagnostics["previous_terminal_updated_at"] == old_updated_at
    runner.signal_store.close()


def test_trade_watch_executor_outcome_updates_normally_without_terminal_reactivation(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot(buy_flow=70.0, volume_impulse=0.5)})
    key = signal_key(signal)
    old_updated_at = "2026-06-04T00:00:00+00:00"
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol=signal.symbol,
        side=signal.side,
        state="TRADE_WATCH",
        action="WATCH",
        reason="old_watch",
        entry_price=signal.entry,
        current_sl=signal.stop_loss,
        diagnostics_json={"watch_marker": "preserve"},
    )
    _force_executor_outcome_updated_at(runner, key, old_updated_at)

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(key)
    diagnostics = json.loads(row["diagnostics_json"])
    assert row_count(runner.signal_store) == 1
    assert row["state"] == "TRADE_WATCH"
    assert row["action"] == "WATCH"
    assert row["updated_at"] != old_updated_at
    assert diagnostics["previous_terminal_outcome_reused"] is False
    assert diagnostics["new_executor_attempt_after_terminal"] is False
    runner.signal_store.close()


def test_testnet_order_path_is_called_only_for_enter_long_after_terminal_outcome(tmp_path: Path) -> None:
    fake = _FakeTestnetOrderExecutor()
    runner = make_testnet_runner(tmp_path, fake)
    blocked_signal = make_signal(
        meta={
            "tf": "5",
            "market": "linear",
            "btc_regime": "BTC_DUMP_RISK",
            "executor_snapshot": make_snapshot(buy_flow=80.0, sell_flow=120.0, volume_impulse=0.6),
        }
    )
    key = signal_key(blocked_signal)
    runner.signal_store.upsert_executor_decision(
        signal_key=key,
        symbol=blocked_signal.symbol,
        side=blocked_signal.side,
        state="EXITED",
        action="STALE_RESET",
        reason="old_stale_reset",
        entry_price=blocked_signal.entry,
        current_sl=blocked_signal.stop_loss,
        diagnostics_json={},
    )

    runner._process_paper_executor(blocked_signal, "linear", "CONFIRMED_LONG")

    blocked_row = runner.signal_store.get_executor_outcome(key)
    assert blocked_row["action"] == "WATCH"
    assert fake.entry_calls == 0

    allowed_signal = make_signal(symbol="BTCUSDT", meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})
    runner._process_paper_executor(allowed_signal, "linear", "CONFIRMED_LONG")

    allowed_row = runner.signal_store.get_executor_outcome(signal_key(allowed_signal))
    assert allowed_row["action"] == "ENTER_LONG"
    assert fake.entry_calls == 1
    runner.signal_store.close()
