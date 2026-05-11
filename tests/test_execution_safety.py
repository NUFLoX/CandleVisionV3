from __future__ import annotations

import asyncio
import json
import os
import sys
import types
import unittest

from fastapi import HTTPException

from api.ws_stream import OrderBookStream
from dashboard.server import verify_ingest_auth


class FakeQueue:
    def __init__(self) -> None:
        self.items = []

    def put_nowait(self, item) -> None:
        self.items.append(item)


class FakeWebSocket:
    closed = False

    def __init__(self, messages: list[dict]) -> None:
        self.messages = [json.dumps(message) for message in messages]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)


class FakeExecutor:
    def __init__(self) -> None:
        self.calls = 0
        self.queue = FakeQueue()

    async def process_signal_async(self, signal_data):
        self.calls += 1


class FakeDB:
    def __init__(self) -> None:
        self.added = []

    def load_open_trades(self):
        return []

    def add_trade(self, trade):
        self.added.append(trade)
        return 1

    def update_trade_status(self, trade_id, status, pnl):
        pass


class ExecutionSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_ws_sniper_queues_signal_without_calling_executor(self) -> None:
        executor = FakeExecutor()
        stream = OrderBookStream(executor=executor)
        stream.sniper_targets["BTCUSDT"] = 100.0
        stream.ws = FakeWebSocket(
            [
                {
                    "topic": "publicTrade.BTCUSDT",
                    "data": [{"s": "BTCUSDT", "p": "101", "v": "120", "S": "Buy"}],
                }
            ]
        )

        await stream._listen()

        self.assertEqual(executor.calls, 0)
        self.assertEqual(len(executor.queue.items), 1)
        self.assertEqual(executor.queue.items[0]["symbol"], "BTCUSDT")

    async def test_signals_only_executor_does_not_write_active_trade(self) -> None:
        charting_stub = types.ModuleType("api.charting")
        charting_stub.generate_setup_chart = lambda *args, **kwargs: ""
        previous_charting = sys.modules.get("api.charting")
        sys.modules["api.charting"] = charting_stub
        from core import executor as executor_module

        original_flag = executor_module.SIGNALS_ONLY
        original_report = executor_module.Executor._send_execution_report
        executor_module.SIGNALS_ONLY = True

        async def noop_report(self, trade, score, df):
            return None

        executor_module.Executor._send_execution_report = noop_report
        try:
            db = FakeDB()
            executor = executor_module.Executor(db)
            executor.warmup_seconds = 0
            result = await executor.process_signal_async(
                {
                    "symbol": "BTCUSDT",
                    "score": 3.0,
                    "entry_price": 100.0,
                    "sl": 95.0,
                    "tp": 110.0,
                    "side": "Buy",
                    "timeframe": "1m",
                    "reasons": ["unit"],
                }
            )
        finally:
            executor_module.SIGNALS_ONLY = original_flag
            executor_module.Executor._send_execution_report = original_report
            if previous_charting is None:
                sys.modules.pop("api.charting", None)
            else:
                sys.modules["api.charting"] = previous_charting

        self.assertTrue(result)
        self.assertEqual(db.added, [])

    def test_refresh_auth_rejects_missing_bearer_token(self) -> None:
        previous = os.environ.get("DASHBOARD_INGEST_TOKEN")
        os.environ["DASHBOARD_INGEST_TOKEN"] = "secret-token"
        try:
            with self.assertRaises(HTTPException):
                verify_ingest_auth(None)
            verify_ingest_auth("Bearer secret-token")
        finally:
            if previous is None:
                os.environ.pop("DASHBOARD_INGEST_TOKEN", None)
            else:
                os.environ["DASHBOARD_INGEST_TOKEN"] = previous


if __name__ == "__main__":
    unittest.main()
