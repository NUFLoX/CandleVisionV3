from __future__ import annotations

import asyncio
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orderflow_accum.signal_store import SignalStore
from tools import outcome_tracker


class FakeDataFrame:
    empty = False

    def to_dict(self, orient: str) -> list[dict[str, float]]:
        assert orient == "records"
        return [{"high": 111.0, "low": 99.0}]


class FakeBybitRestClient:
    client = None

    def __init__(self, *_args, **_kwargs) -> None:
        self.calls: list[tuple[str, str, int, str]] = []
        self.failures: dict[str, list[Exception]] = {}
        FakeBybitRestClient.client = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def fetch_klines(self, symbol: str, *, interval: str, limit: int, category: str):
        self.calls.append((symbol, interval, limit, category))
        failures = self.failures.get(symbol, [])
        if failures:
            raise failures.pop(0)
        return FakeDataFrame()


class FakeSettings:
    rest_base_url = "https://example.invalid"
    rest_timeout_seconds = 1
    rest_retries = 0


def _install_fakes(monkeypatch: pytest.MonkeyPatch, sleep_calls: list[float] | None = None) -> None:
    bybit_rest = ModuleType("orderflow_accum.bybit_rest")
    bybit_rest.BybitRestClient = FakeBybitRestClient
    config = ModuleType("orderflow_accum.config")
    config.Settings = FakeSettings

    FakeBybitRestClient.client = None
    monkeypatch.setitem(sys.modules, "orderflow_accum.bybit_rest", bybit_rest)
    monkeypatch.setitem(sys.modules, "orderflow_accum.config", config)

    async def fake_sleep(seconds: float) -> None:
        if sleep_calls is not None:
            sleep_calls.append(seconds)

    monkeypatch.setattr(outcome_tracker.asyncio, "sleep", fake_sleep)


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "signals.db"
    store = SignalStore(db_path=str(db_path))
    store.close()
    return db_path


def _insert_signal(
    db_path: Path,
    *,
    signal_key: str,
    symbol: str = "BTCUSDT",
    timeframe: str = "1",
    status: str = "PENDING",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO signals (
            signal_key,
            symbol,
            market,
            timeframe,
            source,
            kind,
            side,
            score_first,
            score_last,
            score_max,
            entry,
            stop_loss,
            take_profit_1,
            take_profit_2,
            reasons_first,
            reasons_last,
            meta,
            first_seen,
            last_seen,
            repeat_count,
            status
        )
        VALUES (?, ?, ?, ?, 'test', 'test', 'Buy', 1, 1, 1, 100, 95, 105, 110, '[]', '[]', '{}', ?, ?, 1, ?)
        """,
        (signal_key, symbol, "linear", timeframe, now, now, status),
    )
    conn.commit()
    conn.close()


def _signal_rows(db_path: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM signals ORDER BY id").fetchall()
    conn.close()
    return rows


def test_rate_limit_once_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sleep_calls: list[float] = []
    _install_fakes(monkeypatch, sleep_calls)
    db_path = _make_db(tmp_path)
    _insert_signal(db_path, signal_key="sig-1")

    original_enter = FakeBybitRestClient.__aenter__

    async def enter_with_rate_limit(self):
        self.failures["BTCUSDT"] = [Exception("Bybit API error retCode 10006: Too many visits")]
        return await original_enter(self)

    monkeypatch.setattr(FakeBybitRestClient, "__aenter__", enter_with_rate_limit)

    updated = asyncio.run(outcome_tracker.run_once(str(db_path), lookahead_bars=10, expires_hours=48))

    rows = _signal_rows(db_path)
    assert updated == 1
    assert rows[0]["status"] == "TP2"
    assert FakeBybitRestClient.client is not None
    assert FakeBybitRestClient.client.calls == [("BTCUSDT", "1", 10, "linear"), ("BTCUSDT", "1", 10, "linear")]
    assert 5.0 in sleep_calls
    assert 0.2 in sleep_calls


def test_repeated_symbol_timeframe_uses_candle_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fakes(monkeypatch, [])
    db_path = _make_db(tmp_path)
    _insert_signal(db_path, signal_key="sig-1", symbol="ETHUSDT", timeframe="5")
    _insert_signal(db_path, signal_key="sig-2", symbol="ETHUSDT", timeframe="5")

    updated = asyncio.run(outcome_tracker.run_once(str(db_path), lookahead_bars=20, expires_hours=48))

    rows = _signal_rows(db_path)
    assert updated == 2
    assert [row["status"] for row in rows] == ["TP2", "TP2"]
    assert FakeBybitRestClient.client is not None
    assert FakeBybitRestClient.client.calls == [("ETHUSDT", "5", 20, "linear")]


def test_permanent_failure_skips_signal_without_crashing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sleep_calls: list[float] = []
    _install_fakes(monkeypatch, sleep_calls)
    db_path = _make_db(tmp_path)
    _insert_signal(db_path, signal_key="sig-1", symbol="BADUSDT")
    _insert_signal(db_path, signal_key="sig-2", symbol="GOODUSDT")

    original_enter = FakeBybitRestClient.__aenter__

    async def enter_with_failures(self):
        self.failures["BADUSDT"] = [Exception("Too many visits") for _ in range(4)]
        return await original_enter(self)

    monkeypatch.setattr(FakeBybitRestClient, "__aenter__", enter_with_failures)

    updated = asyncio.run(outcome_tracker.run_once(str(db_path), lookahead_bars=10, expires_hours=48))

    rows = _signal_rows(db_path)
    assert updated == 1
    assert rows[0]["status"] == "PENDING"
    assert rows[1]["status"] == "TP2"
    assert FakeBybitRestClient.client is not None
    assert FakeBybitRestClient.client.calls == [
        ("BADUSDT", "1", 10, "linear"),
        ("BADUSDT", "1", 10, "linear"),
        ("BADUSDT", "1", 10, "linear"),
        ("BADUSDT", "1", 10, "linear"),
        ("GOODUSDT", "1", 10, "linear"),
    ]
    assert sleep_calls.count(5.0) == 3
