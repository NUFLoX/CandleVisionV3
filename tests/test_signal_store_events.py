from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "orderflow_accum" / "signal_store.py"
spec = importlib.util.spec_from_file_location("signal_store_events_direct", MODULE_PATH)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
SignalStore = mod.SignalStore


def test_signal_store_add_event_persists_row(tmp_path: Path) -> None:
    db = tmp_path / "signals.db"
    store = SignalStore(db_path=str(db))

    store.add_event(
        signal_key="BTCUSDT|linear|15|ACCUMULATION_WATCH|Buy",
        symbol="BTCUSDT",
        timeframe="15",
        event_type="outcome_transition",
        from_status="WATCHING",
        to_status="PRE_IMPULSE",
        score_last=8.5,
    )

    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT signal_key, symbol, timeframe, event_type, from_status, to_status, score_last FROM signal_events"
    ).fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "BTCUSDT|linear|15|ACCUMULATION_WATCH|Buy"
    assert row[1] == "BTCUSDT"
    assert row[2] == "15"
    assert row[3] == "outcome_transition"
    assert row[4] == "WATCHING"
    assert row[5] == "PRE_IMPULSE"
    assert row[6] == 8.5
