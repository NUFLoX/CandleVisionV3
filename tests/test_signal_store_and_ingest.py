from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.ingest_client import signal_to_dashboard_payload
from orderflow_accum.signal_store import SignalStore


@dataclass(slots=True)
class FakeSignal:
    symbol: str = "BTCUSDT"
    side: str = "Buy"
    kind: str = "ACCUMULATION_WATCH"
    source: str = "orderflow"
    score: float = 6.2
    entry: float = 100.0
    stop_loss: float = 95.0
    take_profit_1: float = 110.0
    take_profit_2: float = 115.0
    reasons: list[str] = field(default_factory=lambda: ["sell_pressure_absorbed"])
    meta: dict[str, object] = field(default_factory=lambda: {"tf": "15"})


def test_meta_tf_parsing_normalizes_for_dashboard_payload() -> None:
    payload = signal_to_dashboard_payload(FakeSignal())
    assert payload["timeframe"] == "15m"


def test_signal_store_dedupe_repeated_signals_updates_existing_row(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    store = SignalStore(db_path=str(db_path), score_jump_threshold=2.0)

    first = FakeSignal(score=6.0)
    result_1 = store.upsert_signal(first, market="linear")
    assert result_1.is_new is True
    assert result_1.should_notify is True

    repeat = FakeSignal(score=6.4)
    result_2 = store.upsert_signal(repeat, market="linear")
    assert result_2.is_new is False
    assert result_2.repeat_count == 2
    assert result_2.should_notify is False

    jump = FakeSignal(score=8.7)
    result_3 = store.upsert_signal(jump, market="linear")
    assert result_3.is_new is False
    assert result_3.score_jump is True
    assert result_3.should_notify is True

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT COUNT(*) FROM signals").fetchone()
    assert row is not None and row[0] == 1
    details = conn.execute("SELECT repeat_count, score_first, score_last, score_max FROM signals").fetchone()
    conn.close()

    assert details is not None
    assert details[0] == 3
    assert details[1] == 6.0
    assert details[2] == 8.7
    assert details[3] == 8.7
