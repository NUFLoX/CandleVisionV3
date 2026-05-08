from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from dataclasses import dataclass, field

from dashboard.ingest_client import signal_to_dashboard_payload
from dashboard.live_data import _fetch_global_pressure, _fetch_one_coin_metric
from dashboard.schemas import Signal, SignalStrength, SignalType
from dashboard.store import DashboardStore


class DashboardLiveDataTests(unittest.TestCase):
    def test_coingecko_global_pressure_is_parsed_from_mock(self) -> None:
        from dashboard import live_data

        original = live_data._http_get_json
        live_data._http_get_json = lambda url, params: {
            "data": {
                "market_cap_percentage": {"btc": 51.2, "eth": 14.8, "usdt": 4.1},
                "market_cap_change_percentage_24h_usd": 2.5,
            }
        }
        try:
            strips = asyncio.run(_fetch_global_pressure())
        finally:
            live_data._http_get_json = original

        by_key = {strip.key: strip for strip in strips}
        self.assertEqual(by_key["btc_d"].value, 51.2)
        self.assertEqual(by_key["usdt_d"].value, 4.1)
        self.assertEqual(by_key["total3"].value, 34.0)
        self.assertEqual(by_key["btc_cap"].direction, "up")

    def test_bybit_coin_metric_parser_uses_mock_client(self) -> None:
        metric = asyncio.run(_fetch_one_coin_metric(FakeBybit(), "BTCUSDT"))

        self.assertEqual(metric.symbol, "BTCUSDT")
        self.assertGreater(metric.volume_24h_usd, 0)
        self.assertGreaterEqual(metric.rsi, 0)
        self.assertLessEqual(metric.rsi, 100)
        self.assertNotEqual(metric.orderbook_imbalance, 0)
        self.assertIn("Real Bybit data", metric.bot_verdict)

    def test_orderflow_signal_maps_to_dashboard_signal_payload(self) -> None:
        signal = FakeOrderflowSignal()
        payload = signal_to_dashboard_payload(signal)
        parsed = Signal(**payload)

        self.assertEqual(parsed.symbol, "SOLUSDT")
        self.assertEqual(parsed.strength, SignalStrength.strong)
        self.assertEqual(parsed.signal_type, SignalType.confirmed)
        self.assertIn("BREAKOUT_LONG", parsed.reason)

    def test_dashboard_store_persists_ingested_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "dashboard_state.json")
            previous = os.environ.get("DASHBOARD_STATE_PATH")
            os.environ["DASHBOARD_STATE_PATH"] = state_path
            try:
                store = DashboardStore()
                signal = Signal(
                    id="test-signal",
                    symbol="BTCUSDT",
                    exchange="Bybit",
                    timeframe="1m",
                    score=8.1,
                    strength=SignalStrength.strong,
                    signal_type=SignalType.confirmed,
                    entry=100.0,
                    stop_loss=95.0,
                    take_profit_1=110.0,
                    reason="unit test",
                )
                asyncio.run(store.add_signal(signal))

                restored = DashboardStore()
                snapshot = asyncio.run(restored.snapshot())
                self.assertEqual(snapshot.signals[0].id, "test-signal")
            finally:
                if previous is None:
                    os.environ.pop("DASHBOARD_STATE_PATH", None)
                else:
                    os.environ["DASHBOARD_STATE_PATH"] = previous


class FakeBybit:
    async def ticker(self, symbol: str) -> dict:
        return {"lastPrice": "125.5", "turnover24h": "987654321", "price24hPcnt": "0.031"}

    async def kline(self, symbol: str) -> list[dict[str, float]]:
        candles = []
        price = 100.0
        for index in range(220):
            open_price = price
            close = price + (1.2 if index % 4 else -0.6)
            candles.append(
                {
                    "time": float(index),
                    "open": open_price,
                    "high": max(open_price, close) + 0.4,
                    "low": min(open_price, close) - 0.4,
                    "close": close,
                    "volume": 1000.0 + index,
                    "turnover": (1000.0 + index) * close,
                }
            )
            price = close
        return candles

    async def orderbook(self, symbol: str) -> dict:
        return {"result": {"b": [["125", "100"], ["124", "80"]], "a": [["126", "45"], ["127", "35"]]}}


@dataclass(slots=True)
class FakeOrderflowSignal:
    symbol: str = "SOLUSDT"
    side: str = "Buy"
    kind: str = "BREAKOUT_LONG"
    source: str = "orderflow"
    score: float = 8.4
    entry: float = 155.0
    stop_loss: float = 148.0
    take_profit_1: float = 168.0
    take_profit_2: float = 180.0
    reasons: list[str] = field(default_factory=lambda: ["mock breakout", "delta positive"])
    meta: dict[str, object] = field(default_factory=lambda: {"imbalance": 0.24})


if __name__ == "__main__":
    unittest.main()
