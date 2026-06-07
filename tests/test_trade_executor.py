from __future__ import annotations

import math

from orderflow_accum.trade_executor import (
    BTC_BEARISH,
    BTC_DUMP_RISK,
    BUY,
    ENTERED,
    ENTER_LONG,
    ENTER_SHORT,
    EXIT,
    EXITED,
    HOLD,
    MOVE_SL_TO_BREAKEVEN,
    PROTECT_BREAKEVEN,
    SELL,
    WATCH,
    WATCH_ENTRY,
    OrderflowSnapshot,
    SmartTradeExecutor,
    TradeSetup,
)


def make_buy_setup(**overrides):
    data = {
        "symbol": "ETHUSDT",
        "side": BUY,
        "entry_hint": 100.0,
        "stop_loss": 97.0,
        "score": 9.0,
        "timeframe": "5m",
        "btc_regime": "BTC_NEUTRAL",
        "reasons": ["confirmed_long"],
        "created_at": None,
    }
    data.update(overrides)
    return TradeSetup(**data)


def make_sell_setup(**overrides):
    data = {
        "symbol": "ETHUSDT",
        "side": SELL,
        "entry_hint": 100.0,
        "stop_loss": 103.0,
        "score": 9.0,
        "timeframe": "5m",
        "btc_regime": BTC_BEARISH,
        "reasons": ["confirmed_short"],
        "created_at": None,
    }
    data.update(overrides)
    return TradeSetup(**data)


def make_snapshot(**overrides):
    data = {
        "price": 100.0,
        "spread_bps": 8.0,
        "buy_flow": 120.0,
        "sell_flow": 80.0,
        "bid_wall_strength": 0.30,
        "ask_wall_strength": 0.30,
        "volume_impulse": 1.3,
        "support": 99.0,
        "resistance": 101.0,
        "ema20": 99.5,
        "vwap": 99.4,
        "candle_close": None,
        "bars_since_entry": 0,
    }
    data.update(overrides)
    return OrderflowSnapshot(**data)


def test_buy_setup_waits_when_ask_wall_is_strong():
    executor = SmartTradeExecutor()
    setup = make_buy_setup()
    snapshot = make_snapshot(ask_wall_strength=0.90)

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == WATCH
    assert decision.reason == "entry_blocked_ask_wall"
    assert decision.position is None


def test_buy_setup_enters_when_ask_wall_disappears_and_buy_flow_dominates():
    executor = SmartTradeExecutor()
    setup = make_buy_setup()
    snapshot = make_snapshot(
        ask_wall_strength=0.20,
        buy_flow=140.0,
        sell_flow=90.0,
        volume_impulse=1.5,
    )

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == ENTER_LONG
    assert decision.reason == "entry_allowed_long"
    assert decision.next_state == ENTERED



def test_buy_setup_blocks_btc_dump_risk_market_regime_before_new_long_entry():
    executor = SmartTradeExecutor()
    setup = make_buy_setup(btc_regime=BTC_DUMP_RISK)
    snapshot = make_snapshot(buy_flow=140.0, sell_flow=90.0, volume_impulse=1.5)

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == WATCH
    assert decision.reason == "entry_blocked_market_regime"
    assert decision.next_state == WATCH_ENTRY


def test_buy_setup_blocks_btc_bearish_market_regime_before_normal_new_long_entry():
    executor = SmartTradeExecutor()
    setup = make_buy_setup(btc_regime=BTC_BEARISH)
    snapshot = make_snapshot(buy_flow=140.0, sell_flow=90.0, volume_impulse=1.5)

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == WATCH
    assert decision.reason == "entry_blocked_market_regime"
    assert decision.next_state == WATCH_ENTRY

def test_buy_position_moves_sl_to_breakeven_after_half_r_confirmation():
    executor = SmartTradeExecutor()
    setup = make_buy_setup(stop_loss=97.0)
    entry_snapshot = make_snapshot(price=100.0, support=99.0)
    position = executor.open_position(setup, entry_snapshot)

    assert math.isclose(position.initial_risk, 3.0, rel_tol=0.0, abs_tol=1e-9)

    update_snapshot = make_snapshot(
        price=101.5,
        buy_flow=130.0,
        sell_flow=100.0,
        volume_impulse=1.1,
        bars_since_entry=1,
    )

    decision = executor.update_position(position, update_snapshot)

    assert decision.action == MOVE_SL_TO_BREAKEVEN
    assert decision.reason == "sl_moved_to_breakeven"
    assert decision.next_state == PROTECT_BREAKEVEN
    assert decision.position is not None
    assert math.isclose(decision.position.current_sl, 100.1, rel_tol=0.0, abs_tol=1e-9)


def test_buy_position_exits_when_sell_flow_dominates():
    executor = SmartTradeExecutor()
    setup = make_buy_setup()
    entry_snapshot = make_snapshot(price=100.0, support=99.0)
    position = executor.open_position(setup, entry_snapshot)

    update_snapshot = make_snapshot(
        price=99.8,
        buy_flow=80.0,
        sell_flow=110.0,
        bars_since_entry=1,
    )

    decision = executor.update_position(position, update_snapshot)

    assert decision.action == EXIT
    assert decision.reason == "exit_sell_flow_dominance"
    assert decision.next_state == EXITED
    assert decision.position is not None
    assert decision.position.exit_price == 99.8


def test_buy_position_holds_through_shallow_pullback_if_support_holds():
    executor = SmartTradeExecutor()
    setup = make_buy_setup()
    entry_snapshot = make_snapshot(price=100.0, support=99.0)
    position = executor.open_position(setup, entry_snapshot)

    update_snapshot = make_snapshot(
        price=99.7,
        support=99.0,
        ema20=99.4,
        buy_flow=100.0,
        sell_flow=95.0,
        ask_wall_strength=0.40,
        volume_impulse=0.95,
        bars_since_entry=1,
    )

    decision = executor.update_position(position, update_snapshot)

    assert decision.action == HOLD
    assert decision.reason == "hold_position"
    assert decision.position is not None
    assert decision.position.state == ENTERED
    assert decision.position.exit_price is None


def test_sell_setup_enters_only_when_btc_bearish_and_sell_flow_dominates():
    executor = SmartTradeExecutor()
    setup = make_sell_setup(btc_regime=BTC_BEARISH)
    snapshot = make_snapshot(
        price=100.0,
        buy_flow=80.0,
        sell_flow=120.0,
        bid_wall_strength=0.20,
        resistance=101.0,
        ema20=100.5,
        vwap=100.4,
    )

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == ENTER_SHORT
    assert decision.reason == "entry_allowed_short"


def test_sell_setup_blocked_when_btc_is_not_bearish():
    executor = SmartTradeExecutor()
    setup = make_sell_setup(btc_regime="BTC_NEUTRAL")
    snapshot = make_snapshot(
        price=100.0,
        buy_flow=80.0,
        sell_flow=120.0,
        bid_wall_strength=0.20,
        resistance=101.0,
        ema20=100.5,
        vwap=100.4,
    )

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == WATCH
    assert decision.reason == "entry_blocked_btc_regime"


def test_sell_position_exits_when_buy_flow_dominates():
    executor = SmartTradeExecutor()
    setup = make_sell_setup()
    entry_snapshot = make_snapshot(
        price=100.0,
        buy_flow=80.0,
        sell_flow=120.0,
        resistance=101.0,
        ema20=100.5,
        vwap=100.4,
    )
    position = executor.open_position(setup, entry_snapshot)

    update_snapshot = make_snapshot(
        price=100.4,
        buy_flow=130.0,
        sell_flow=90.0,
        resistance=101.0,
        ema20=100.2,
        vwap=100.1,
        bars_since_entry=1,
    )

    decision = executor.update_position(position, update_snapshot)

    assert decision.action == EXIT
    assert decision.reason == "exit_buy_flow_dominance"
    assert decision.position is not None
    assert decision.position.exit_price == 100.4


def test_stop_loss_hit_exits_position():
    executor = SmartTradeExecutor()
    setup = make_buy_setup(stop_loss=97.0)
    entry_snapshot = make_snapshot(price=100.0, support=99.0)
    position = executor.open_position(setup, entry_snapshot)

    update_snapshot = make_snapshot(
        price=96.9,
        buy_flow=90.0,
        sell_flow=100.0,
        bars_since_entry=1,
    )

    decision = executor.update_position(position, update_snapshot)

    assert decision.action == EXIT
    assert decision.reason == "exit_stop_loss_hit"
    assert decision.next_state == EXITED


def test_max_gain_r_and_max_drawdown_r_update_correctly():
    executor = SmartTradeExecutor()
    setup = make_buy_setup(stop_loss=97.0)
    entry_snapshot = make_snapshot(price=100.0, support=None)
    position = executor.open_position(setup, entry_snapshot)

    first_update = make_snapshot(
        price=101.5,
        buy_flow=125.0,
        sell_flow=100.0,
        volume_impulse=0.9,
        support=99.0,
        bars_since_entry=1,
    )
    first_decision = executor.update_position(position, first_update)

    assert first_decision.action == HOLD
    assert first_decision.position is not None
    assert math.isclose(first_decision.position.max_gain_r, 0.5, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(first_decision.position.max_drawdown_r, 0.0, rel_tol=0.0, abs_tol=1e-9)

    second_update = make_snapshot(
        price=99.4,
        buy_flow=100.0,
        sell_flow=95.0,
        support=99.0,
        ema20=99.2,
        bars_since_entry=1,
    )
    second_decision = executor.update_position(first_decision.position, second_update)

    assert second_decision.action == HOLD
    assert second_decision.position is not None
    assert math.isclose(second_decision.position.max_gain_r, 0.5, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(second_decision.position.max_drawdown_r, 0.2, rel_tol=0.0, abs_tol=1e-9)

def test_sell_setup_blocked_when_score_is_low():
    executor = SmartTradeExecutor()
    setup = make_sell_setup(score=5.0)
    snapshot = make_snapshot(
        price=100.0,
        buy_flow=80.0,
        sell_flow=120.0,
        bid_wall_strength=0.20,
        resistance=101.0,
        ema20=100.5,
        vwap=100.4,
    )

    decision = executor.evaluate_entry(setup, snapshot)

    assert decision.action == WATCH
    assert decision.reason == "entry_blocked_low_score"
    assert decision.position is None