from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.exchange_gateway import BybitGateway
from core.execution.safe_executor import InMemoryStateStore, OrderManager, OrderStatus


class FakeSession:
    def place_order(self, **kwargs):
        return {"retCode": 0, "result": {"orderId": "oid-1"}}


class FakeExchange:
    def __init__(self) -> None:
        self.calls = 0

    async def place_order(self, payload):
        self.calls += 1
        return {"retCode": 0, "result": {"orderId": "oid-1"}}


def test_exchange_gateway_blocks_when_trading_disabled(monkeypatch):
    monkeypatch.setattr("api.exchange_gateway.trading_enabled", lambda: False)
    gateway = BybitGateway(FakeSession())
    result = asyncio.run(gateway.place_order({"symbol": "BTCUSDT", "side": "Buy", "qty": "1", "price": "100", "stopLoss": "95", "takeProfit": "110", "orderLinkId": "x"}))
    assert result["retCode"] == -1
    assert result["retMsg"] == "trading_disabled"


def test_safe_order_manager_blocks_when_trading_disabled(monkeypatch):
    monkeypatch.setattr("core.execution.safe_executor.trading_enabled", lambda: False)
    store = InMemoryStateStore()
    manager = OrderManager(FakeExchange(), store)
    status = asyncio.run(manager.create_order("BTCUSDT", "Buy", 1, 100, 95, 110))
    assert status == OrderStatus.FAILED
