from __future__ import annotations

import unittest
from datetime import datetime, timezone

from dashboard.schemas import Signal, SignalStrength, SignalType
from dashboard.signal_outcomes import aggregate_signal_stats, calculate_r, calculate_signal_outcome


def make_signal(**overrides) -> Signal:
    payload = {
        "id": "sig-1",
        "symbol": "BTCUSDT",
        "exchange": "Bybit",
        "timeframe": "1m",
        "score": 8.0,
        "strength": SignalStrength.strong,
        "signal_type": SignalType.confirmed,
        "entry": 100.0,
        "stop_loss": 95.0,
        "take_profit_1": 110.0,
        "reason": "unit test",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    payload.update(overrides)
    return Signal(**payload)


def candle(high: float, low: float, close: float = 100.0, start: int = 1_767_225_600_000) -> dict[str, float]:
    return {"start": start, "open": 100.0, "high": high, "low": low, "close": close, "volume": 1.0, "turnover": 100.0}


class SignalOutcomeTests(unittest.TestCase):
    def test_long_tp_before_sl(self) -> None:
        outcome = calculate_signal_outcome(make_signal(), [candle(111.0, 99.0), candle(100.0, 94.0)])
        self.assertEqual(outcome.outcome, "tp")
        self.assertEqual(outcome.r_multiple, 2.0)

    def test_long_sl_before_tp(self) -> None:
        outcome = calculate_signal_outcome(make_signal(), [candle(101.0, 94.0), candle(111.0, 99.0)])
        self.assertEqual(outcome.outcome, "sl")
        self.assertEqual(outcome.r_multiple, -1.0)

    def test_short_tp_before_sl(self) -> None:
        signal = make_signal(entry=100.0, stop_loss=105.0, take_profit_1=90.0)
        outcome = calculate_signal_outcome(signal, [candle(101.0, 89.0), candle(106.0, 99.0)])
        self.assertEqual(outcome.direction, "short")
        self.assertEqual(outcome.outcome, "tp")
        self.assertEqual(outcome.r_multiple, 2.0)

    def test_ambiguous_same_candle(self) -> None:
        outcome = calculate_signal_outcome(make_signal(), [candle(111.0, 94.0)])
        self.assertEqual(outcome.outcome, "ambiguous")
        self.assertEqual(outcome.r_multiple, 0.0)

    def test_expired(self) -> None:
        outcome = calculate_signal_outcome(make_signal(), [candle(104.0, 97.0, close=103.0)], max_bars=1)
        self.assertEqual(outcome.outcome, "expired")
        self.assertEqual(outcome.r_multiple, 0.6)

    def test_r_calculation(self) -> None:
        self.assertEqual(calculate_r(make_signal(), 107.5), 1.5)
        self.assertEqual(calculate_r(make_signal(entry=100.0, stop_loss=105.0, take_profit_1=90.0), 92.5), 1.5)

    def test_aggregate_stats(self) -> None:
        outcomes = [
            calculate_signal_outcome(make_signal(id="win"), [candle(111.0, 99.0)]),
            calculate_signal_outcome(make_signal(id="loss"), [candle(101.0, 94.0)]),
            calculate_signal_outcome(make_signal(id="amb"), [candle(111.0, 94.0)]),
        ]
        stats = aggregate_signal_stats(outcomes)
        self.assertEqual(stats.total, 3)
        self.assertEqual(stats.wins, 1)
        self.assertEqual(stats.losses, 1)
        self.assertEqual(stats.ambiguous, 1)
        self.assertEqual(stats.win_rate, 0.5)
        self.assertEqual(stats.total_r, 1.0)
        self.assertTrue(stats.by_symbol)


if __name__ == "__main__":
    unittest.main()
