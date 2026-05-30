from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
import sys

import pytest

from orderflow_accum.models import Signal
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_learning import TradeLearningEngine


@dataclass
class FakeUpsert:
    is_new: bool
    to_status: str
    repeat_count: int = 1
    status_changed: bool = False
    score_jump: bool = False
    from_status: str | None = None


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


def _install_runner_import_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    dashboard_ingest = ModuleType("dashboard.ingest_client")
    dashboard_ingest.DashboardIngestClient = type("DashboardIngestClient", (), {})
    monkeypatch.setitem(sys.modules, "dashboard.ingest_client", dashboard_ingest)

    bybit_rest = ModuleType("orderflow_accum.bybit_rest")
    bybit_rest.BybitRestClient = type("BybitRestClient", (), {})
    bybit_rest.ScanTarget = type("ScanTarget", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.bybit_rest", bybit_rest)

    console_ui = ModuleType("orderflow_accum.console_ui")
    console_ui.ConsoleUI = type("ConsoleUI", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.console_ui", console_ui)

    engines = ModuleType("orderflow_accum.engines")
    engines.MacroAccumulationEngine = type("MacroAccumulationEngine", (), {})
    engines.RealtimeAccumulationEngine = type("RealtimeAccumulationEngine", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.engines", engines)

    short_engine = ModuleType("orderflow_accum.short_engine")
    short_engine.DistributionShortEngine = type("DistributionShortEngine", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.short_engine", short_engine)

    market_regime = ModuleType("orderflow_accum.market_regime")
    market_regime.MarketRegimeAnalyzer = type("MarketRegimeAnalyzer", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.market_regime", market_regime)

    chart_render = ModuleType("orderflow_accum.chart_render")
    chart_render.render_signal_chart = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "orderflow_accum.chart_render", chart_render)

    signal_logger = ModuleType("orderflow_accum.signal_logger")
    signal_logger.RejectionCsvLogger = type("RejectionCsvLogger", (), {})
    signal_logger.SignalCsvLogger = type("SignalCsvLogger", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.signal_logger", signal_logger)

    telegram_notify = ModuleType("orderflow_accum.telegram_notify")
    telegram_notify.TelegramNotifier = type("TelegramNotifier", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.telegram_notify", telegram_notify)

    ws_clients = ModuleType("orderflow_accum.ws_clients")
    ws_clients.MarketStream = type("MarketStream", (), {})
    monkeypatch.setitem(sys.modules, "orderflow_accum.ws_clients", ws_clients)


@pytest.fixture(autouse=True)
def runner_import_stubs(monkeypatch: pytest.MonkeyPatch):
    _install_runner_import_stubs(monkeypatch)
    yield


def test_trade_learning_engine_records_signal_created_and_updated(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    signal = make_signal()
    key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"

    engine.record_signal(signal=signal, signal_key=key, event_type="SIGNAL_CREATED", status="CONFIRMED_LONG")
    engine.record_signal(signal=signal, signal_key=key, event_type="SIGNAL_UPDATED", status="CONFIRMED_LONG")

    rows = store.get_trade_lifecycle_events(key)
    assert [row["event_type"] for row in rows] == ["SIGNAL_CREATED", "SIGNAL_UPDATED"]
    assert rows[0]["score"] == 9.0
    assert rows[0]["btc_regime"] == "BTC_NEUTRAL"
    assert rows[0]["features"]["reasons"] == ["long_promotion_rules_met"]
    store.close()


def test_trade_learning_engine_records_executor_action_mapping(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    signal = make_signal()
    key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"

    engine.record_executor_decision(signal=signal, signal_key=key, state="TRADE_WATCH", action="WATCH", reason="wait")
    engine.record_executor_decision(signal=signal, signal_key=key, state="ENTERED", action="ENTER_LONG", reason="enter")
    engine.record_executor_decision(signal=signal, signal_key=key, state="EXITED", action="EXIT", reason="exit")

    rows = store.get_trade_lifecycle_events(key)
    assert [row["event_type"] for row in rows] == ["EXECUTOR_WATCH", "EXECUTOR_ENTER", "EXECUTOR_EXIT"]
    assert [row["action"] for row in rows] == ["WATCH", "ENTER_LONG", "EXIT"]
    store.close()


def test_trade_learning_engine_swallows_store_failures() -> None:
    class BrokenStore:
        def add_trade_lifecycle_event(self, event):
            raise RuntimeError("boom")

    engine = TradeLearningEngine(BrokenStore())

    engine.record_event(
        {
            "signal_key": "bad",
            "symbol": "BAD",
            "event_type": "SIGNAL_CREATED",
        }
    )


def test_runner_lifecycle_helper_uses_fake_learning_without_touching_signal_state() -> None:
    from orderflow_accum.runner import AccumulationRunner

    class FakeLearning:
        def __init__(self) -> None:
            self.events = []

        def record_signal(self, **kwargs) -> None:
            self.events.append(kwargs)

    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.trade_learning = FakeLearning()
    signal = make_signal()
    key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"

    runner._record_signal_lifecycle(signal, key, FakeUpsert(True, "CONFIRMED_LONG"), "CONFIRMED_LONG")

    assert [event["event_type"] for event in runner.trade_learning.events] == ["SIGNAL_CREATED", "CONFIRMED"]
    assert signal.kind == "CONFIRMED_LONG"
