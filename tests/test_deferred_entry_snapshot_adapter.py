from __future__ import annotations

from orderflow_accum.deferred_entry_snapshot_adapter import (
    build_deferred_entry_snapshot,
)
from orderflow_accum.trade_executor import OrderflowSnapshot


def _record():
    return {
        "signal_key": (
            "TESTUSDT|linear|60|PRE_IMPULSE_ZONE|Buy"
        ),
        "origin_support": 94.0,
        "origin_ema20": 96.0,
        "origin_vwap": 96.5,
        "metadata_json": {
            "initial_snapshot": {
                "support": 93.0,
                "ema20": 95.0,
                "vwap": 95.5,
            }
        },
    }


def _orderflow_snapshot(
    *,
    price: float = 101.0,
    buy_flow: float = 130.0,
    sell_flow: float = 100.0,
    volume_impulse: float = 1.20,
    ask_wall_strength: float = 0.20,
    support: float | None = 95.0,
    ema20: float | None = 96.0,
    vwap: float | None = 96.5,
):
    return OrderflowSnapshot(
        price=price,
        spread_bps=4.0,
        buy_flow=buy_flow,
        sell_flow=sell_flow,
        bid_wall_strength=0.10,
        ask_wall_strength=ask_wall_strength,
        volume_impulse=volume_impulse,
        support=support,
        resistance=110.0,
        ema20=ema20,
        vwap=vwap,
        candle_close=price,
    )


def test_live_orderflow_keeps_live_price_and_closed_h1_structure():
    result = build_deferred_entry_snapshot(
        _record(),
        orderflow_snapshot=_orderflow_snapshot(),
        closed_h1_structure={
            "price": 100.5,
            "candle_close": 100.5,
            "support": 97.0,
            "ema20": 98.0,
            "vwap": 98.5,
        },
    )

    assert result.snapshot is not None
    assert result.reason == (
        "deferred_entry_snapshot_live_orderflow"
    )
    assert result.used_live_orderflow is True
    assert result.used_closed_h1_structure is True

    snapshot = result.snapshot
    assert snapshot.price == 101.0
    assert snapshot.buy_flow == 130.0
    assert snapshot.sell_flow == 100.0
    assert snapshot.volume_impulse == 1.20
    assert snapshot.ask_wall_strength == 0.20
    assert snapshot.support == 97.0
    assert snapshot.ema20 == 98.0
    assert snapshot.vwap == 98.5
    assert snapshot.candle_close == 100.5


def test_closed_h1_price_fallback_is_conservative_without_live_flow():
    result = build_deferred_entry_snapshot(
        _record(),
        orderflow_snapshot=None,
        closed_h1_structure={
            "price": 95.0,
            "candle_close": 95.0,
            "support": 94.0,
            "ema20": 96.0,
        },
    )

    assert result.snapshot is not None
    assert result.reason == (
        "deferred_entry_snapshot_closed_h1_price_fallback"
    )
    assert result.used_live_orderflow is False

    snapshot = result.snapshot
    assert snapshot.price == 95.0
    assert snapshot.buy_flow == 0.0
    assert snapshot.sell_flow == 0.0
    assert snapshot.volume_impulse == 0.0
    assert snapshot.ask_wall_strength == 1.0
    assert snapshot.support == 94.0
    assert snapshot.ema20 == 96.0
    assert snapshot.vwap == 96.5


def test_missing_live_and_closed_price_returns_no_snapshot():
    result = build_deferred_entry_snapshot(
        _record(),
        orderflow_snapshot=None,
        closed_h1_structure={
            "support": 94.0,
            "ema20": 96.0,
        },
    )

    assert result.snapshot is None
    assert result.reason == (
        "deferred_entry_snapshot_missing_price"
    )
    assert result.used_live_orderflow is False


def test_snapshot_adapter_has_no_execution_surface():
    assert not hasattr(
        build_deferred_entry_snapshot,
        "open_position",
    )
    assert not hasattr(
        build_deferred_entry_snapshot,
        "execute_order",
    )
    assert not hasattr(
        build_deferred_entry_snapshot,
        "submit_order",
    )
