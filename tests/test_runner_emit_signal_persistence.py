from __future__ import annotations

import asyncio
import importlib
import logging
import sqlite3
import sys
import time
from pathlib import Path
from types import ModuleType

from orderflow_accum.models import Signal
from orderflow_accum.signal_store import SignalStore


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


def _module(**attrs) -> ModuleType:
    module = ModuleType("stub")
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


def _import_runner_with_stubs():
    stubbed = {
        "orderflow_accum.bybit_rest": _module(BybitRestClient=_StubBybitRestClient, ScanTarget=_StubScanTarget),
        "orderflow_accum.console_ui": _module(ConsoleUI=_StubService),
        "orderflow_accum.engines": _module(MacroAccumulationEngine=_StubService, RealtimeAccumulationEngine=_StubService),
        "orderflow_accum.short_engine": _module(DistributionShortEngine=_StubService),
        "orderflow_accum.market_regime": _module(MarketRegimeAnalyzer=_StubService),
        "orderflow_accum.signal_logger": _module(RejectionCsvLogger=_StubService, SignalCsvLogger=_StubService),
        "orderflow_accum.confirmed_promoter": _module(ConfirmedPromoter=_StubService),
        "orderflow_accum.telegram_notify": _module(TelegramNotifier=_StubService),
        "orderflow_accum.ws_clients": _module(MarketStream=_StubService),
        "orderflow_accum.chart_render": _module(render_signal_chart=lambda *args, **kwargs: None),
        "orderflow_accum.indicators": _module(add_indicators=lambda df: df),
    }
    saved = {name: sys.modules.get(name) for name in [*stubbed, "orderflow_accum.runner"]}
    for name, module in stubbed.items():
        sys.modules[name] = module
    sys.modules.pop("orderflow_accum.runner", None)
    runner_module = importlib.import_module("orderflow_accum.runner")

    def restore() -> None:
        sys.modules.pop("orderflow_accum.runner", None)
        for name, module in saved.items():
            if name == "orderflow_accum.runner":
                continue
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
        if saved["orderflow_accum.runner"] is not None:
            sys.modules["orderflow_accum.runner"] = saved["orderflow_accum.runner"]

    return runner_module.AccumulationRunner, restore


class CountingSignalStore(SignalStore):
    def __init__(self, db_path: str) -> None:
        super().__init__(db_path=db_path)
        self.upsert_calls = 0

    def upsert_signal(self, signal, *, market: str = "linear"):
        self.upsert_calls += 1
        return super().upsert_signal(signal, market=market)


class DummySettings:
    market_categories = ["linear"]
    signal_cooldown_seconds = 60
    macro_symbol_cooldown_minutes = 30
    telegram_send_charts = False


class FakeLearning:
    def __init__(self) -> None:
        self.events = []

    def record_signal(self, **kwargs) -> None:
        self.events.append(kwargs)


class FakePromoter:
    def __init__(self) -> None:
        self.calls = 0

    def should_promote(self, *args, **kwargs):
        self.calls += 1
        return type("Decision", (), {"should_promote": False, "target_status": None, "reasons": ["not_enough_repeats"]})()


class NoopDashboard:
    async def post_signal(self, signal) -> None:  # pragma: no cover - guarded by cooldown in this test
        raise AssertionError("dashboard signal should be skipped by cooldown")

    async def post_log(self, *args, **kwargs) -> None:  # pragma: no cover - guarded by cooldown in this test
        raise AssertionError("dashboard log should be skipped by cooldown")


class NoopTelegram:
    async def send_signal(self, *args, **kwargs) -> None:  # pragma: no cover - guarded by cooldown in this test
        raise AssertionError("telegram should be skipped by cooldown")


def make_signal() -> Signal:
    return Signal(
        symbol="ETHUSDT",
        side="Buy",
        kind="ACCUMULATION_WATCH",
        source="orderflow",
        score=7.5,
        entry=100.0,
        stop_loss=98.0,
        take_profit_1=104.0,
        take_profit_2=108.0,
        reasons=["test_emit_once"],
        meta={"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL"},
    )


def make_runner(accumulation_runner_cls, store: CountingSignalStore):
    runner = accumulation_runner_cls.__new__(accumulation_runner_cls)
    runner.settings = DummySettings()
    runner.logger = logging.getLogger("test.emit_signal")
    runner.orderflow_logger = logging.getLogger("test.emit_signal.orderflow")
    runner.macro_logger = logging.getLogger("test.emit_signal.macro")
    runner.signal_store = store
    runner.promoter = FakePromoter()
    runner.trade_learning = FakeLearning()
    runner.trade_executor_mode = "paper"
    runner.trade_executor_enabled = False
    runner.trade_executor = None
    runner.dashboard = NoopDashboard()
    runner.telegram = NoopTelegram()
    runner._cooldowns = {}
    runner._counts = {"macro": 0, "orderflow": 0}
    return runner


def repeat_count(db_path: Path, signal_key: str) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        return int(conn.execute("SELECT repeat_count FROM signals WHERE signal_key = ?", (signal_key,)).fetchone()[0])
    finally:
        conn.close()


def test_emit_signal_upserts_and_checks_promotion_once_without_double_repeat(tmp_path: Path) -> None:
    AccumulationRunner, restore_runner_imports = _import_runner_with_stubs()
    try:
        db_path = tmp_path / "signals.db"
        store = CountingSignalStore(str(db_path))
        runner = make_runner(AccumulationRunner, store)
        signal = make_signal()
        market = "linear"
        signal_key = runner._signal_key(signal, market)

        initial = store.upsert_signal(signal, market=market)
        assert initial.repeat_count == 1
        assert repeat_count(db_path, signal_key) == 1
        store.upsert_calls = 0

        cooldown_key = f"{signal.dedupe_key()}|{signal.meta['tf']}"
        runner._cooldowns[cooldown_key] = time.time()

        asyncio.run(runner._emit_signal(rest=None, signal=signal))

        assert store.upsert_calls == 1
        assert runner.promoter.calls == 1
        assert repeat_count(db_path, signal_key) == 2
        assert [event["event_type"] for event in runner.trade_learning.events] == ["SIGNAL_UPDATED"]
        assert runner.trade_learning.events[0]["features"]["repeat_count"] == 2
        store.close()
    finally:
        restore_runner_imports()
