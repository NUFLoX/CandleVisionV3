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
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def fetch_klines(self, symbol: str, *, interval: str, limit: int, category: str):
        return FakeDataFrame()


class FakeSettings:
    rest_base_url = "https://example.invalid"
    rest_timeout_seconds = 1
    rest_retries = 0


def _install_fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    bybit_rest = ModuleType("orderflow_accum.bybit_rest")
    bybit_rest.BybitRestClient = FakeBybitRestClient
    config = ModuleType("orderflow_accum.config")
    config.Settings = FakeSettings
    monkeypatch.setitem(sys.modules, "orderflow_accum.bybit_rest", bybit_rest)
    monkeypatch.setitem(sys.modules, "orderflow_accum.config", config)

    async def fake_sleep(seconds: float) -> None:
        return None

    monkeypatch.setattr(outcome_tracker.asyncio, "sleep", fake_sleep)


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "signals.db"
    store = SignalStore(db_path=str(db_path))
    store.close()
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO signals (
            signal_key, symbol, market, timeframe, source, kind, side,
            score_first, score_last, score_max, entry, stop_loss,
            take_profit_1, take_profit_2, reasons_first, reasons_last,
            meta, first_seen, last_seen, repeat_count, status
        )
        VALUES (
            'sig-1', 'BTCUSDT', 'linear', '1', 'test', 'test', 'Buy',
            1, 1, 1, 100, 95, 105, 110, '[]', '["reason"]',
            '{"tf":"1"}', ?, ?, 1, 'PENDING'
        )
        """,
        (now, now),
    )
    conn.commit()
    conn.close()
    return db_path


def test_outcome_tracker_learning_call_is_best_effort(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fakes(monkeypatch)
    db_path = _make_db(tmp_path)

    from orderflow_accum.trade_learning import TradeLearningEngine

    def broken_record_outcome(self, signal, signal_key: str, outcome: str, outcome_features: dict) -> None:
        raise RuntimeError("learning unavailable")

    monkeypatch.setattr(TradeLearningEngine, "record_outcome", broken_record_outcome)

    updated = asyncio.run(outcome_tracker.run_once(str(db_path), lookahead_bars=10, expires_hours=48))

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT status, outcome FROM signals WHERE signal_key='sig-1'").fetchone()
    conn.close()
    assert updated == 1
    assert row == ("TP2", "TP2")
