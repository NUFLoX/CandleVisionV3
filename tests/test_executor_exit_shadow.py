from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from types import ModuleType


def _stub_module(name: str, **attrs) -> None:
    module = ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module


class _StubService:
    def __init__(self, *args, **kwargs):
        pass


class _StubBybitRestClient(_StubService):
    pass


class _StubScanTarget:
    def __init__(self, symbol: str = "", market: str = "") -> None:
        self.symbol = symbol
        self.market = market


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

from orderflow_accum.executor_exit_shadow import evaluate_exit_shadow_policy
from orderflow_accum.models import Signal
from orderflow_accum.runner import AccumulationRunner
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_executor import HOLD, EXIT, EXITED, TradeDecision, TradePosition, SmartTradeExecutor

for _module_name in (
    "dashboard.ingest_client",
    "orderflow_accum.bybit_rest",
    "orderflow_accum.console_ui",
    "orderflow_accum.engines",
    "orderflow_accum.short_engine",
    "orderflow_accum.market_regime",
    "orderflow_accum.signal_logger",
    "orderflow_accum.confirmed_promoter",
    "orderflow_accum.telegram_notify",
    "orderflow_accum.ws_clients",
    "orderflow_accum.chart_render",
):
    sys.modules.pop(_module_name, None)


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
        "stop_loss": 90.0,
        "take_profit_1": 120.0,
        "take_profit_2": 140.0,
        "reasons": ["long_promotion_rules_met"],
        "meta": {"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL"},
    }
    data.update(overrides)
    return Signal(**data)


def make_runner(tmp_path: Path) -> AccumulationRunner:
    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.settings = DummySettings()
    runner.logger = logging.getLogger("test.executor_exit_shadow")
    runner.signal_store = SignalStore(db_path=str(tmp_path / "signals.db"))
    runner.trade_executor_enabled = True
    runner.trade_executor = SmartTradeExecutor()
    runner.executor_exit_shadow_enabled = True
    runner.executor_exit_shadow_policy = "trailing_40pct_giveback_after_1r"
    return runner


def test_policy_triggers_after_40pct_giveback_from_1r_peak() -> None:
    evaluation = evaluate_exit_shadow_policy(previous_peak_r=1.2, current_r=0.7)

    assert evaluation.floor_r == 0.72
    assert evaluation.triggered is True
    assert evaluation.exit_r == 0.72


def test_policy_does_not_trigger_below_1r_peak() -> None:
    evaluation = evaluate_exit_shadow_policy(previous_peak_r=0.9, current_r=0.4)

    assert evaluation.floor_r is None
    assert evaluation.triggered is False
    assert evaluation.exit_r is None


def _position() -> TradePosition:
    return TradePosition(
        symbol="ETHUSDT",
        side="Buy",
        state="ENTERED",
        entry_price=100.0,
        stop_loss=90.0,
        current_sl=90.0,
        max_price=112.0,
        min_price=100.0,
        max_gain_r=1.2,
        max_drawdown_r=0.0,
        bars_in_trade=3,
        exit_price=None,
        exit_reason=None,
        initial_risk=10.0,
    )


def test_open_executor_outcome_diagnostics_updated_when_shadow_enabled(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear"})
    position = _position()
    snapshot = type("Snapshot", (), {"price": 107.0})()
    decision = TradeDecision(HOLD, "hold_position", "ENTERED", position)

    row = runner._store_paper_executor_decision("ETHUSDT|linear|5|CONFIRMED_LONG|Buy", signal, decision, position, snapshot)
    diagnostics = json.loads(row["diagnostics_json"])

    assert diagnostics["exit_shadow_enabled"] is True
    assert diagnostics["exit_shadow_triggered"] is True
    assert diagnostics["exit_shadow_peak_r"] == 1.2
    assert diagnostics["exit_shadow_floor_r"] == 0.72
    assert diagnostics["exit_shadow_current_r"] == 0.7
    assert diagnostics["exit_shadow_exit_r"] == 0.72
    runner.signal_store.close()


def test_shadow_trigger_lifecycle_event_written_once(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal_key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"
    signal = make_signal(meta={"tf": "5", "market": "linear"})
    position = _position()
    snapshot = type("Snapshot", (), {"price": 107.0})()
    decision = TradeDecision(HOLD, "hold_position", "ENTERED", position)

    runner._store_paper_executor_decision(signal_key, signal, decision, position, snapshot)
    runner._store_paper_executor_decision(signal_key, signal, decision, position, snapshot)

    events = [event for event in runner.signal_store.get_trade_lifecycle_events(signal_key) if event["event_type"] == "EXECUTOR_SHADOW_EXIT"]
    assert len(events) == 1
    assert events[0]["status"] == "SHADOW_EXIT"
    assert events[0]["action"] == "SHADOW_TRAILING_EXIT"
    runner.signal_store.close()


def test_closed_executor_trade_carries_shadow_diagnostics(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal_key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"
    signal = make_signal(meta={"tf": "5", "market": "linear"})
    position = _position()
    open_snapshot = type("Snapshot", (), {"price": 107.0})()
    open_decision = TradeDecision(HOLD, "hold_position", "ENTERED", position)
    previous_row = runner._store_paper_executor_decision(signal_key, signal, open_decision, position, open_snapshot)

    exit_position = TradePosition(**{**position.__dict__, "state": EXITED, "exit_price": 95.0, "exit_reason": "exit_stop_loss_hit"})
    exit_decision = TradeDecision(EXIT, "exit_stop_loss_hit", EXITED, exit_position)
    row = runner._store_paper_executor_decision(signal_key, signal, exit_decision, exit_position, type("Snapshot", (), {"price": 95.0})())

    trades = runner.signal_store.list_executor_trades()
    assert len(trades) == 1
    diagnostics = json.loads(trades[0]["diagnostics_json"])
    assert diagnostics["exit_shadow_policy"] == "trailing_40pct_giveback_after_1r"
    assert diagnostics["exit_shadow_exit_r"] == 0.72
    assert diagnostics["exit_shadow_actual_r"] == -1.0
    assert diagnostics["exit_shadow_delta_r"] == 1.72
    assert previous_row["signal_key"] == row["signal_key"]
    runner.signal_store.close()
