from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path
from types import ModuleType

import pytest

from orderflow_accum.models import Signal
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_executor import SmartTradeExecutor


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


class DummySettings:
    market_categories = ["linear"]


class FakeTradeLearning:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def record_executor_decision(self, **kwargs) -> None:
        self.events.append(kwargs)


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
        "buy_flow": 100.0,
        "sell_flow": 90.0,
        "bid_wall_strength": 0.2,
        "ask_wall_strength": 0.2,
        "volume_impulse": 1.1,
        "support": 99.5,
        "resistance": 102.0,
        "ema20": 99.8,
        "vwap": 99.9,
        "bars_since_entry": 0,
    }
    data.update(overrides)
    return data


def make_runner(tmp_path: Path):
    from orderflow_accum.runner import AccumulationRunner

    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.settings = DummySettings()
    runner.logger = logging.getLogger("test.executor_snapshot_diagnostics")
    runner.signal_store = SignalStore(db_path=str(tmp_path / "signals.db"))
    runner.trade_executor_enabled = True
    runner.trade_executor_mode = "paper"
    runner.trade_executor = SmartTradeExecutor()
    runner.trade_learning = FakeTradeLearning()
    return runner


def signal_key(signal: Signal) -> str:
    return f"{signal.symbol}|linear|{signal.meta['tf']}|{signal.kind}|{signal.side}"


def test_executor_outcomes_schema_adds_diagnostic_columns(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))

    columns = {row[1] for row in store.conn.execute("PRAGMA table_info(executor_outcomes)").fetchall()}

    assert {
        "price",
        "spread_bps",
        "buy_flow",
        "sell_flow",
        "required_buy_flow",
        "required_sell_flow",
        "volume_impulse",
        "required_volume_impulse",
        "bid_wall_strength",
        "ask_wall_strength",
        "support",
        "resistance",
        "ema20",
        "vwap",
        "diagnostics_json",
    }.issubset(columns)
    store.close()


def test_executor_schema_migrates_existing_executor_outcomes_table(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE executor_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            state TEXT NOT NULL,
            action TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    store = SignalStore(db_path=str(db_path))
    columns = {row[1] for row in store.conn.execute("PRAGMA table_info(executor_outcomes)").fetchall()}

    assert "volume_impulse" in columns
    assert "required_volume_impulse" in columns
    assert "diagnostics_json" in columns
    store.close()


def test_runner_persists_executor_snapshot_diagnostics_and_lifecycle_features(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": make_snapshot()})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert row is not None
    assert row["volume_impulse"] == 1.1
    assert row["required_volume_impulse"] == 1.2
    assert row["buy_flow"] == 100.0
    assert row["sell_flow"] == 90.0
    assert row["required_buy_flow"] == 90.0 * runner.trade_executor.flow_ratio
    assert row["spread_bps"] == 4.0
    assert row["ask_wall_strength"] == 0.2
    assert row["bid_wall_strength"] == 0.2
    diagnostics = json.loads(row["diagnostics_json"])
    assert diagnostics["flow_ratio"] == runner.trade_executor.flow_ratio
    assert diagnostics["max_spread_bps"] == runner.trade_executor.max_spread_bps

    assert runner.trade_learning.events
    features = runner.trade_learning.events[0]["features"]
    assert features["volume_impulse"] == 1.1
    assert features["required_volume_impulse"] == 1.2
    assert features["required_buy_flow"] == 90.0 * runner.trade_executor.flow_ratio
    runner.signal_store.close()


def test_missing_snapshot_diagnostics_do_not_crash_and_store_nulls(tmp_path: Path) -> None:
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "executor_snapshot": {"price": 100.0, "buy_flow": None}})

    runner._process_paper_executor(signal, "linear", "CONFIRMED_LONG")

    row = runner.signal_store.get_executor_outcome(signal_key(signal))
    assert row is not None
    assert row["buy_flow"] is None
    assert row["sell_flow"] is None
    assert row["required_buy_flow"] is None
    assert row["volume_impulse"] is None
    assert row["required_volume_impulse"] == runner.trade_executor.min_entry_volume_impulse
    runner.signal_store.close()
