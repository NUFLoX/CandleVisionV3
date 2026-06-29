from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd

from orderflow_accum.deferred_entry import (
    DEFERRED_ENTRY_PENDING,
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
    DeferredEntryCandidate,
    DeferredEntryStore,
)
from orderflow_accum.deferred_entry_refresh_service import (
    DeferredEntryRefreshService,
)
from orderflow_accum.deferred_entry_runtime import (
    DeferredEntryRuntime,
    DeferredEntryRuntimeConfig,
)
from orderflow_accum.deferred_entry_service import (
    DeferredEntryCoordinator,
)
from orderflow_accum.runner import AccumulationRunner
from orderflow_accum.trade_executor import OrderflowSnapshot


NOW = datetime(2026, 6, 29, 0, 0, tzinfo=timezone.utc)
KEY = "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"


class FakeLogger:
    def debug(self, *args, **kwargs):
        del args, kwargs

    def info(self, *args, **kwargs):
        del args, kwargs

    def exception(self, *args, **kwargs):
        del args, kwargs


class FakeStream:
    def __init__(self, state):
        self.state = state

    def get_state(self, symbol):
        del symbol
        return self.state


class FakeRest:
    def __init__(self, frame):
        self.frame = frame
        self.calls = []

    async def fetch_klines(
        self,
        symbol,
        *,
        interval,
        limit,
        category,
    ):
        self.calls.append(
            (symbol, interval, limit, category)
        )
        return self.frame.copy()


def _candidate() -> DeferredEntryCandidate:
    return DeferredEntryCandidate(
        signal_key=KEY,
        symbol="TESTUSDT",
        market="linear",
        timeframe="60",
        side="Buy",
        signal_kind="PRE_IMPULSE_ZONE",
        origin_entry=100.0,
        origin_stop_loss=90.0,
        score=12.0,
        initial_block_reason="entry_blocked_buy_flow",
        created_at=NOW,
        expires_at=NOW + timedelta(hours=24),
        origin_support=94.0,
        origin_ema20=96.0,
        origin_vwap=96.5,
    )


def _snapshot(
    *,
    price: float,
    buy_flow: float,
    sell_flow: float,
    volume_impulse: float,
) -> OrderflowSnapshot:
    return OrderflowSnapshot(
        price=price,
        spread_bps=4.0,
        buy_flow=buy_flow,
        sell_flow=sell_flow,
        bid_wall_strength=0.10,
        ask_wall_strength=0.20,
        volume_impulse=volume_impulse,
        support=94.0,
        resistance=110.0,
        ema20=96.0,
        vwap=96.5,
        candle_close=price,
    )


def _fresh_state(*, price: float = 100.0):
    return SimpleNamespace(
        snapshots=[
            SimpleNamespace(
                mid=price,
                spread_bps=4.0,
                ts=time.time(),
            )
        ],
        trades=[SimpleNamespace(notional=1.0)],
    )


def _runner(tmp_path, *, mode: str = "paper"):
    runner = AccumulationRunner.__new__(AccumulationRunner)

    store = DeferredEntryStore(
        str(tmp_path / "signals.db")
    )
    coordinator = DeferredEntryCoordinator(store)

    runner.deferred_entry_store = store
    runner.deferred_entry_runtime = DeferredEntryRuntime(
        coordinator,
        config=DeferredEntryRuntimeConfig(
            enabled=True,
            ttl_hours=24.0,
            h1_only=True,
        ),
    )
    runner.deferred_entry_refresh_service = (
        DeferredEntryRefreshService(
            coordinator,
            max_active=12,
        )
    )
    runner._deferred_entry_structure_cache = {}
    runner.trade_executor_enabled = True
    runner.trade_executor_mode = mode
    runner.logger = FakeLogger()

    return runner, coordinator, store


def _closed_h1_structure():
    return {
        "price": 96.0,
        "candle_close": 96.0,
        "support": 94.0,
        "ema20": 96.0,
    }


def test_runner_refresh_advances_to_ready_without_opening_position(
    tmp_path,
):
    runner, coordinator, store = _runner(tmp_path)

    try:
        registration = coordinator.register(_candidate())
        assert registration is not None

        snapshots = iter(
            [
                _snapshot(
                    price=95.0,
                    buy_flow=100.0,
                    sell_flow=100.0,
                    volume_impulse=0.80,
                ),
                _snapshot(
                    price=97.0,
                    buy_flow=120.0,
                    sell_flow=100.0,
                    volume_impulse=0.80,
                ),
            ]
        )

        runner._paper_executor_snapshot = (
            lambda signal, state: (next(snapshots), False)
        )

        async def closed_structure(rest, record):
            del rest, record
            return _closed_h1_structure()

        runner._deferred_entry_closed_h1_structure = (
            closed_structure
        )

        runner._derive_volume_impulse = (
            lambda *args, **kwargs: {
                "volume_impulse_missing": False,
            }
        )

        def fail_if_opened(*args, **kwargs):
            raise AssertionError(
                "refresh lifecycle must not open a position"
            )

        runner._open_executor_position = fail_if_opened
        stream = FakeStream(_fresh_state(price=95.0))

        first = asyncio.run(
            runner.refresh_deferred_entry_candidates(
                rest=object(),
                stream=stream,
            )
        )

        assert first == 1
        row = store.get(KEY)
        assert row is not None
        assert row["status"] == (
            DEFERRED_ENTRY_PULLBACK_SEEN
        )

        second = asyncio.run(
            runner.refresh_deferred_entry_candidates(
                rest=object(),
                stream=stream,
            )
        )

        assert second == 1
        row = store.get(KEY)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_READY
    finally:
        store.close()


def test_runner_refresh_uses_zero_flow_when_live_snapshot_is_weak(
    tmp_path,
):
    runner, coordinator, store = _runner(tmp_path)

    try:
        registration = coordinator.register(_candidate())
        assert registration is not None

        runner._paper_executor_snapshot = (
            lambda signal, state: (
                _snapshot(
                    price=95.0,
                    buy_flow=1.0,
                    sell_flow=1.0,
                    volume_impulse=1.0,
                ),
                True,
            )
        )

        async def closed_structure(rest, record):
            del rest, record
            return _closed_h1_structure()

        runner._deferred_entry_closed_h1_structure = (
            closed_structure
        )

        refreshed = asyncio.run(
            runner.refresh_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(
                    _fresh_state(price=95.0)
                ),
            )
        )

        assert refreshed == 1
        row = store.get(KEY)
        assert row is not None
        assert row["status"] == (
            DEFERRED_ENTRY_PULLBACK_SEEN
        )
        assert row["last_snapshot_json"]["buy_flow"] == 0.0
        assert row["last_snapshot_json"]["sell_flow"] == 0.0
        assert row["last_snapshot_json"][
            "volume_impulse"
        ] == 0.0
        assert row["last_snapshot_json"][
            "ask_wall_strength"
        ] == 1.0
    finally:
        store.close()


def test_runner_refresh_uses_closed_h1_fallback_when_orderflow_is_stale(
    tmp_path,
):
    runner, coordinator, store = _runner(tmp_path)

    try:
        registration = coordinator.register(_candidate())
        assert registration is not None

        async def closed_structure(rest, record):
            del rest, record
            return _closed_h1_structure()

        runner._deferred_entry_closed_h1_structure = (
            closed_structure
        )

        def fail_if_stale_orderflow_is_used(*args, **kwargs):
            raise AssertionError(
                "stale orderflow must not be used for deferred refresh"
            )

        runner._paper_executor_snapshot = (
            fail_if_stale_orderflow_is_used
        )

        stale_state = _fresh_state(price=95.0)
        stale_state.snapshots[-1].ts = time.time() - 3600.0

        refreshed = asyncio.run(
            runner.refresh_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(stale_state),
            )
        )

        assert refreshed == 1

        row = store.get(KEY)
        assert row is not None
        assert row["status"] == (
            DEFERRED_ENTRY_PULLBACK_SEEN
        )
        assert row["last_snapshot_json"]["buy_flow"] == 0.0
        assert row["last_snapshot_json"]["sell_flow"] == 0.0
        assert row["last_snapshot_json"][
            "volume_impulse"
        ] == 0.0
        assert row["last_snapshot_json"][
            "ask_wall_strength"
        ] == 1.0
    finally:
        store.close()


def test_runner_refresh_does_nothing_outside_paper_mode(
    tmp_path,
):
    runner, coordinator, store = _runner(
        tmp_path,
        mode="testnet",
    )

    try:
        registration = coordinator.register(_candidate())
        assert registration is not None

        refreshed = asyncio.run(
            runner.refresh_deferred_entry_candidates(
                rest=object(),
                stream=FakeStream(_fresh_state()),
            )
        )

        assert refreshed == 0
        row = store.get(KEY)
        assert row is not None
        assert row["status"] == DEFERRED_ENTRY_PENDING
    finally:
        store.close()


def test_closed_h1_structure_skips_forming_candle_and_uses_cache(
    tmp_path,
):
    del tmp_path

    runner = AccumulationRunner.__new__(AccumulationRunner)
    runner.logger = FakeLogger()
    runner._deferred_entry_structure_cache = {}
    runner._env_float = lambda name, default: 300.0

    rows = []
    for index in range(30):
        close = 100.0 + index
        rows.append(
            {
                "start": f"2026-06-29T{index:02d}:00:00Z",
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1000.0 + index,
                "turnover": 100000.0 + index,
            }
        )

    rows[-1]["close"] = 999.0
    rows[-1]["high"] = 1000.0
    rows[-1]["low"] = 998.0

    rest = FakeRest(pd.DataFrame(rows))
    record = {
        "symbol": "TESTUSDT",
        "market": "linear",
        "timeframe": "60",
    }

    first = asyncio.run(
        runner._deferred_entry_closed_h1_structure(
            rest,
            record,
        )
    )
    second = asyncio.run(
        runner._deferred_entry_closed_h1_structure(
            rest,
            record,
        )
    )

    assert first is not None
    assert second is not None
    assert first["candle_close"] == 128.0
    assert first["candle_close"] != 999.0
    assert first == second
    assert rest.calls == [
        ("TESTUSDT", "60", 30, "linear")
    ]
