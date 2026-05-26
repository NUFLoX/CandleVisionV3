from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Stub optional dependency so api.bybit_client import doesn't require pybit in this environment.
if "pybit.unified_trading" not in sys.modules:
    pybit_mod = types.ModuleType("pybit")
    unified = types.ModuleType("pybit.unified_trading")

    class _HTTP:  # pragma: no cover - simple stub
        def __init__(self, *args, **kwargs):
            pass

    unified.HTTP = _HTTP
    sys.modules["pybit"] = pybit_mod
    sys.modules["pybit.unified_trading"] = unified

from api.bybit_client import BybitClient


class FakeSession:
    def __init__(self) -> None:
        self.place_calls = 0

    def place_order(self, **kwargs):
        self.place_calls += 1
        return {"result": {"orderId": "x"}}

    def set_trading_stop(self, **kwargs):
        raise AssertionError("set_trading_stop should not be called when trading is disabled")


def test_execute_long_trade_blocked_when_trading_disabled(monkeypatch) -> None:
    monkeypatch.setattr("api.bybit_client.trading_enabled", lambda: False)
    c = BybitClient()
    c.session = FakeSession()

    ok = c.execute_long_trade("BTCUSDT", qty=1.0, entry_price=100.0, sl_price=95.0, tp_price=110.0)

    assert ok is False
    assert c.session.place_calls == 0
