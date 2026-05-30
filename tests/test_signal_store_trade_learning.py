from __future__ import annotations

import sqlite3
from pathlib import Path

from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_learning import TradeLifecycleEvent


def test_signal_store_creates_trade_lifecycle_events_table(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))

    row = store.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='trade_lifecycle_events'"
    ).fetchone()

    assert row is not None
    store.close()


def test_add_and_get_trade_lifecycle_event_round_trips_features(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    event = TradeLifecycleEvent(
        signal_key="BTCUSDT|linear|5|CONFIRMED_LONG|Buy",
        symbol="BTCUSDT",
        timeframe="5",
        side="Buy",
        event_type="SIGNAL_CREATED",
        status="CONFIRMED_LONG",
        action=None,
        reason="unit_test",
        price=100.0,
        score=9.5,
        btc_regime="BTC_NEUTRAL",
        market_regime="RANGE",
        features={"reasons": ["support_defended"], "nested": {"x": 1}},
        created_at="2026-05-01T00:00:00+00:00",
    )

    store.add_trade_lifecycle_event(event)
    rows = store.get_trade_lifecycle_events(event.signal_key)

    assert len(rows) == 1
    assert rows[0]["event_type"] == "SIGNAL_CREATED"
    assert rows[0]["features"] == {"reasons": ["support_defended"], "nested": {"x": 1}}
    store.close()


def test_get_trade_lifecycle_events_orders_by_created_at_then_id(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"

    store.add_trade_lifecycle_event(
        {"signal_key": key, "symbol": "ETHUSDT", "event_type": "SIGNAL_UPDATED", "created_at": "2026-05-01T00:00:02+00:00"}
    )
    store.add_trade_lifecycle_event(
        {"signal_key": key, "symbol": "ETHUSDT", "event_type": "SIGNAL_CREATED", "created_at": "2026-05-01T00:00:01+00:00"}
    )
    store.add_trade_lifecycle_event(
        {"signal_key": key, "symbol": "ETHUSDT", "event_type": "CONFIRMED", "created_at": "2026-05-01T00:00:02+00:00"}
    )

    rows = store.get_trade_lifecycle_events(key)

    assert [row["event_type"] for row in rows] == ["SIGNAL_CREATED", "SIGNAL_UPDATED", "CONFIRMED"]
    store.close()


def test_non_json_safe_features_do_not_crash(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    key = "SOLUSDT|linear|5|CONFIRMED_LONG|Buy"

    store.add_trade_lifecycle_event(
        {
            "signal_key": key,
            "symbol": "SOLUSDT",
            "event_type": "SIGNAL_CREATED",
            "features": {"bad": {1, 2, 3}, "conn": sqlite3.connect(":memory:")},
        }
    )
    rows = store.get_trade_lifecycle_events(key)

    assert len(rows) == 1
    assert sorted(rows[0]["features"]["bad"]) == [1, 2, 3]
    assert "sqlite3.Connection" in rows[0]["features"]["conn"]
    store.close()
