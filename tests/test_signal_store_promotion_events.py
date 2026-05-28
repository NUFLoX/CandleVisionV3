from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orderflow_accum.models import Signal
from orderflow_accum.signal_store import SignalStore


def test_signal_store_promotion_event(tmp_path) -> None:
    db = tmp_path / "signals.db"
    store = SignalStore(db_path=str(db))
    sig = Signal(
        symbol="BTCUSDT", side="Buy", kind="PRE_IMPULSE_ZONE", source="orderflow", score=9.0,
        entry=1.0, stop_loss=0.9, take_profit_1=1.1, take_profit_2=1.2,
        reasons=["sell_pressure_absorbed"], meta={"tf": "15"},
    )
    store.upsert_signal(sig, market="linear")
    key = "BTCUSDT|linear|15|PRE_IMPULSE_ZONE|Buy"
    changed = store.promote_signal(signal_key=key, to_status="CONFIRMED_LONG")
    assert changed is True
    row = store.conn.execute("SELECT status FROM signals WHERE signal_key=?", (key,)).fetchone()
    assert row[0] == "CONFIRMED_LONG"
    evt = store.conn.execute("SELECT event_type, to_status FROM signal_events WHERE signal_key=? ORDER BY id DESC LIMIT 1", (key,)).fetchone()
    assert evt[0] == "promoted_to_confirmed"
    assert evt[1] == "CONFIRMED_LONG"