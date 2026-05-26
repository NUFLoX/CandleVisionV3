from __future__ import annotations

import importlib.util
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "orderflow_accum" / "signal_store.py"
spec = importlib.util.spec_from_file_location("signal_store_direct", MODULE_PATH)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
SignalStore = mod.SignalStore


@dataclass(slots=True)
class FakeSignal:
    symbol: str = "BTCUSDT"
    side: str = "Buy"
    kind: str = "ACCUMULATION_WATCH"
    source: str = "orderflow"
    score: float = 6.0
    entry: float = 100.0
    stop_loss: float = 95.0
    take_profit_1: float = 110.0
    take_profit_2: float = 115.0
    reasons: list[str] = field(default_factory=lambda: ["r1"])
    meta: dict[str, object] = field(default_factory=lambda: {"tf": "15"})


def test_signal_key_uses_symbol_market_tf_kind_side(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    store = SignalStore(db_path=str(db_path))

    s1 = FakeSignal(symbol="BTCUSDT", kind="ACCUMULATION_WATCH", side="Buy", meta={"tf": "15"})
    assert store.upsert_signal(s1, market="linear").is_new is True

    s1_repeat = FakeSignal(symbol="BTCUSDT", kind="ACCUMULATION_WATCH", side="Buy", meta={"tf": "15"})
    assert store.upsert_signal(s1_repeat, market="linear").is_new is False

    s_tf = FakeSignal(symbol="BTCUSDT", kind="ACCUMULATION_WATCH", side="Buy", meta={"tf": "5"})
    assert store.upsert_signal(s_tf, market="linear").is_new is True

    s_market = FakeSignal(symbol="BTCUSDT", kind="ACCUMULATION_WATCH", side="Buy", meta={"tf": "15"})
    assert store.upsert_signal(s_market, market="spot").is_new is True

    s_kind = FakeSignal(symbol="BTCUSDT", kind="PRE_IMPULSE_ZONE", side="Buy", meta={"tf": "15"})
    assert store.upsert_signal(s_kind, market="linear").is_new is True

    s_side = FakeSignal(symbol="BTCUSDT", kind="ACCUMULATION_WATCH", side="Sell", meta={"tf": "15"})
    assert store.upsert_signal(s_side, market="linear").is_new is True

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    conn.close()

    assert count == 5
