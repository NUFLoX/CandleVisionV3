from __future__ import annotations

import math

from orderflow_accum.trade_executor import (
    BUY,
    ENTERED,
    EXIT,
    EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R,
    HOLD,
    MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R,
    MOVE_SL_TO_BREAKEVEN,
    PROTECT_BREAKEVEN,
    SELL,
    TRAILING_PROFIT,
    OrderflowSnapshot,
    SmartTradeExecutor,
    TradeSetup,
)


def make_executor() -> SmartTradeExecutor:
    return SmartTradeExecutor(
        management_policy=MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R,
        protect_after_1r=True,
        min_protected_r_after_1r=0.25,
    )


def make_buy_setup() -> TradeSetup:
    return TradeSetup(
        symbol="ETHUSDT",
        side=BUY,
        entry_hint=100.0,
        stop_loss=97.0,
        score=9.0,
        timeframe="5m",
        btc_regime="BTC_NEUTRAL",
        reasons=["confirmed_long"],
    )


def make_sell_setup() -> TradeSetup:
    return TradeSetup(
        symbol="ETHUSDT",
        side=SELL,
        entry_hint=100.0,
        stop_loss=103.0,
        score=9.0,
        timeframe="5m",
        btc_regime="BTC_BEARISH",
        reasons=["confirmed_short"],
    )


def make_snapshot(**overrides) -> OrderflowSnapshot:
    data = {
        "price": 100.0,
        "spread_bps": 4.0,
        "buy_flow": 140.0,
        "sell_flow": 100.0,
        "bid_wall_strength": 0.2,
        "ask_wall_strength": 0.2,
        "volume_impulse": 1.4,
        "support": 99.0,
        "resistance": 101.0,
        "ema20": None,
        "vwap": None,
        "bars_since_entry": 0,
    }
    data.update(overrides)
    return OrderflowSnapshot(**data)


def test_buy_reaches_1r_and_trailing_protection_activates() -> None:
    executor = make_executor()
    position = executor.open_position(make_buy_setup(), make_snapshot(price=100.0, support=99.0))

    decision = executor.update_position(position, make_snapshot(price=103.0, bars_since_entry=1))

    assert decision.action == HOLD
    assert decision.next_state == TRAILING_PROFIT
    assert decision.position is not None
    assert math.isclose(decision.position.max_gain_r, 1.0, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(decision.position.current_sl, 100.75, rel_tol=0.0, abs_tol=1e-9)


def test_buy_gives_back_40pct_after_1r_and_exits_with_management_reason() -> None:
    executor = make_executor()
    position = executor.open_position(make_buy_setup(), make_snapshot(price=100.0, support=99.0))
    activated = executor.update_position(position, make_snapshot(price=103.0, bars_since_entry=1)).position
    assert activated is not None

    decision = executor.update_position(activated, make_snapshot(price=101.79, bars_since_entry=2))

    assert decision.action == EXIT
    assert decision.reason == EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R
    assert decision.position is not None
    assert decision.position.exit_reason == EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R


def test_buy_after_1r_cannot_return_to_full_minus_1r_because_sl_is_protected() -> None:
    executor = make_executor()
    position = executor.open_position(make_buy_setup(), make_snapshot(price=100.0, support=99.0))
    activated = executor.update_position(position, make_snapshot(price=103.0, bars_since_entry=1)).position
    assert activated is not None

    decision = executor.update_position(activated, make_snapshot(price=97.0, bars_since_entry=2))

    assert decision.action == EXIT
    assert decision.reason == EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R
    assert decision.position is not None
    assert math.isclose(decision.position.current_sl, 100.75, rel_tol=0.0, abs_tol=1e-9)


def test_sell_reaches_1r_and_trailing_protection_activates() -> None:
    executor = make_executor()
    position = executor.open_position(
        make_sell_setup(),
        make_snapshot(price=100.0, buy_flow=100.0, sell_flow=140.0, resistance=101.0),
    )

    decision = executor.update_position(
        position,
        make_snapshot(price=97.0, buy_flow=100.0, sell_flow=140.0, bars_since_entry=1),
    )

    assert decision.action == HOLD
    assert decision.next_state == TRAILING_PROFIT
    assert decision.position is not None
    assert math.isclose(decision.position.max_gain_r, 1.0, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(decision.position.current_sl, 99.25, rel_tol=0.0, abs_tol=1e-9)


def test_sell_gives_back_40pct_after_1r_and_exits_with_management_reason() -> None:
    executor = make_executor()
    position = executor.open_position(
        make_sell_setup(),
        make_snapshot(price=100.0, buy_flow=100.0, sell_flow=140.0, resistance=101.0),
    )
    activated = executor.update_position(
        position,
        make_snapshot(price=97.0, buy_flow=100.0, sell_flow=140.0, bars_since_entry=1),
    ).position
    assert activated is not None

    decision = executor.update_position(
        activated,
        make_snapshot(price=98.21, buy_flow=100.0, sell_flow=140.0, bars_since_entry=2),
    )

    assert decision.action == EXIT
    assert decision.reason == EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R
    assert decision.position is not None
    assert decision.position.exit_reason == EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R


def test_trade_below_1r_keeps_legacy_breakeven_behavior() -> None:
    executor = make_executor()
    position = executor.open_position(make_buy_setup(), make_snapshot(price=100.0, support=99.0))

    decision = executor.update_position(
        position,
        make_snapshot(price=101.5, buy_flow=130.0, sell_flow=100.0, volume_impulse=1.1, bars_since_entry=1),
    )

    assert decision.action == MOVE_SL_TO_BREAKEVEN
    assert decision.next_state == PROTECT_BREAKEVEN
    assert decision.position is not None
    assert math.isclose(decision.position.max_gain_r, 0.5, rel_tol=0.0, abs_tol=1e-9)
    assert math.isclose(decision.position.current_sl, 100.1, rel_tol=0.0, abs_tol=1e-9)


def test_management_v2_does_not_run_when_policy_is_not_enabled() -> None:
    executor = SmartTradeExecutor()
    position = executor.open_position(make_buy_setup(), make_snapshot(price=100.0, support=99.0))

    decision = executor.update_position(position, make_snapshot(price=103.0, bars_since_entry=1))

    assert decision.action == MOVE_SL_TO_BREAKEVEN
    assert decision.position is not None
    assert decision.position.state == PROTECT_BREAKEVEN
    assert math.isclose(decision.position.current_sl, 100.1, rel_tol=0.0, abs_tol=1e-9)
