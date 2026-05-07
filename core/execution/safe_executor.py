import asyncio
import uuid
from enum import Enum
from typing import Dict, Optional


class OrderStatus(str, Enum):
    NEW = "NEW"
    FILLED = "FILLED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class InMemoryStateStore:
    def __init__(self):
        self.orders: Dict[str, dict] = {}
        self.positions: Dict[str, dict] = {}

    def save_order(self, order_id: str, data: dict):
        self.orders[order_id] = data

    def has_active_position(self, symbol: str) -> bool:
        return symbol in self.positions


class OrderManager:
    def __init__(self, exchange, store: InMemoryStateStore):
        self.exchange = exchange
        self.store = store
        self.locks: Dict[str, asyncio.Lock] = {}

    def _lock(self, symbol: str):
        if symbol not in self.locks:
            self.locks[symbol] = asyncio.Lock()
        return self.locks[symbol]

    async def create_order(self, symbol, side, qty, price, sl, tp):
        async with self._lock(symbol):
            if self.store.has_active_position(symbol):
                return OrderStatus.FAILED

            order_link_id = str(uuid.uuid4())

            payload = {
                "symbol": symbol,
                "side": side,
                "qty": str(qty),
                "price": str(price),
                "stopLoss": str(sl),
                "takeProfit": str(tp),
                "orderLinkId": order_link_id,
            }

            response = await self.exchange.place_order(payload)

            if response.get("retCode") != 0:
                return OrderStatus.FAILED

            order_id = response["result"]["orderId"]

            self.store.save_order(order_id, {"symbol": symbol})

            return OrderStatus.FILLED


class SafeExecutor:
    def __init__(self, order_manager: OrderManager):
        self.order_manager = order_manager

    async def execute_signal(self, signal: dict) -> bool:
        status = await self.order_manager.create_order(
            signal["symbol"],
            signal["side"],
            signal["qty"],
            signal["price"],
            signal["sl"],
            signal["tp"],
        )
        return status == OrderStatus.FILLED
