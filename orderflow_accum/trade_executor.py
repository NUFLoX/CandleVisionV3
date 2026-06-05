from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Optional, Sequence


WAIT_SIGNAL = "WAIT_SIGNAL"
WATCH_ENTRY = "WATCH_ENTRY"
ENTERED = "ENTERED"
PROTECT_BREAKEVEN = "PROTECT_BREAKEVEN"
TRAILING_PROFIT = "TRAILING_PROFIT"
EXITED = "EXITED"

WATCH = "WATCH"
ENTER_LONG = "ENTER_LONG"
ENTER_SHORT = "ENTER_SHORT"
MOVE_SL_TO_BREAKEVEN = "MOVE_SL_TO_BREAKEVEN"
HOLD = "HOLD"
EXIT = "EXIT"

BUY = "Buy"
SELL = "Sell"

BTC_BEARISH = "BTC_BEARISH"
BTC_DUMP_RISK = "BTC_DUMP_RISK"

MANAGEMENT_POLICY_LEGACY = "legacy"
MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R = "trailing_40pct_giveback_after_1r"
EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R = "exit_trailing_40pct_giveback_after_1r"


@dataclass(frozen=True)
class TradeSetup:
    symbol: str
    side: str
    entry_hint: float
    stop_loss: float
    score: float
    timeframe: str
    btc_regime: str
    reasons: Sequence[str]
    created_at: Optional[str] = None


@dataclass(frozen=True)
class OrderflowSnapshot:
    price: float
    spread_bps: float
    buy_flow: float
    sell_flow: float
    bid_wall_strength: float
    ask_wall_strength: float
    volume_impulse: float
    support: Optional[float]
    resistance: Optional[float]
    ema20: Optional[float] = None
    vwap: Optional[float] = None
    candle_close: Optional[float] = None
    bars_since_entry: Optional[int] = None


@dataclass(frozen=True)
class TradePosition:
    symbol: str
    side: str
    state: str
    entry_price: float
    stop_loss: float
    current_sl: float
    max_price: float
    min_price: float
    max_gain_r: float
    max_drawdown_r: float
    bars_in_trade: int
    exit_price: Optional[float]
    exit_reason: Optional[str]
    initial_risk: float


@dataclass(frozen=True)
class TradeDecision:
    action: str
    reason: str
    next_state: str
    position: Optional[TradePosition] = None


class SmartTradeExecutor:
    def __init__(
        self,
        *,
        min_long_score: float = 8.0,
        max_spread_bps: float = 15.0,
        flow_ratio: float = 1.15,
        strong_reversal_ratio: float = 1.25,
        min_entry_volume_impulse: float = 1.2,
        min_breakeven_volume_impulse: float = 1.0,
        ask_wall_entry_limit: float = 0.65,
        bid_wall_entry_limit: float = 0.65,
        strong_wall_exit_threshold: float = 0.85,
        buy_fee_buffer_multiplier: float = 1.001,
        sell_fee_buffer_multiplier: float = 0.999,
        support_buffer_multiplier: float = 0.997,
        resistance_buffer_multiplier: float = 1.003,
        management_policy: str = MANAGEMENT_POLICY_LEGACY,
        protect_after_1r: bool = False,
        min_protected_r_after_1r: float = 0.25,
    ) -> None:
        self.min_long_score = min_long_score
        self.max_spread_bps = max_spread_bps
        self.flow_ratio = flow_ratio
        self.strong_reversal_ratio = strong_reversal_ratio
        self.min_entry_volume_impulse = min_entry_volume_impulse
        self.min_breakeven_volume_impulse = min_breakeven_volume_impulse
        self.ask_wall_entry_limit = ask_wall_entry_limit
        self.bid_wall_entry_limit = bid_wall_entry_limit
        self.strong_wall_exit_threshold = strong_wall_exit_threshold
        self.buy_fee_buffer_multiplier = buy_fee_buffer_multiplier
        self.sell_fee_buffer_multiplier = sell_fee_buffer_multiplier
        self.support_buffer_multiplier = support_buffer_multiplier
        self.resistance_buffer_multiplier = resistance_buffer_multiplier
        self.management_policy = str(management_policy or MANAGEMENT_POLICY_LEGACY).strip().lower()
        self.protect_after_1r = bool(protect_after_1r)
        self.min_protected_r_after_1r = max(float(min_protected_r_after_1r), 0.0)

    def evaluate_entry(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> TradeDecision:
        if setup.side == BUY:
            blockers = self._long_entry_blockers(setup, snapshot)
            if blockers:
                return TradeDecision(WATCH, blockers[0], WATCH_ENTRY, None)
            return TradeDecision(ENTER_LONG, "entry_allowed_long", ENTERED, None)

        if setup.side == SELL:
            blockers = self._short_entry_blockers(setup, snapshot)
            if blockers:
                return TradeDecision(WATCH, blockers[0], WATCH_ENTRY, None)
            return TradeDecision(ENTER_SHORT, "entry_allowed_short", ENTERED, None)

        return TradeDecision(WATCH, "entry_blocked_unknown_side", WATCH_ENTRY, None)

    def open_position(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> TradePosition:
        entry_decision = self.evaluate_entry(setup, snapshot)
        if entry_decision.action not in {ENTER_LONG, ENTER_SHORT}:
            raise ValueError(f"cannot open position: {entry_decision.reason}")

        entry_price = float(snapshot.price)
        initial_sl = self._initial_stop_loss(setup, snapshot)
        initial_risk = self._calculate_initial_risk(setup.side, entry_price, initial_sl)

        return TradePosition(
            symbol=setup.symbol,
            side=setup.side,
            state=ENTERED,
            entry_price=entry_price,
            stop_loss=initial_sl,
            current_sl=initial_sl,
            max_price=entry_price,
            min_price=entry_price,
            max_gain_r=0.0,
            max_drawdown_r=0.0,
            bars_in_trade=0,
            exit_price=None,
            exit_reason=None,
            initial_risk=initial_risk,
        )

    def update_position(self, position: TradePosition, snapshot: OrderflowSnapshot) -> TradeDecision:
        if position.state == EXITED:
            return TradeDecision(HOLD, "position_already_exited", EXITED, position)

        updated = self._refresh_position_metrics(position, snapshot)
        updated, management_exit_reason = self._apply_management_v2(updated, snapshot)
        if management_exit_reason is not None:
            exited = replace(
                updated,
                state=EXITED,
                exit_price=float(snapshot.price),
                exit_reason=management_exit_reason,
            )
            return TradeDecision(EXIT, management_exit_reason, EXITED, exited)

        exit_reason = self._exit_reason(updated, snapshot)
        if exit_reason is not None:
            exited = replace(
                updated,
                state=EXITED,
                exit_price=float(snapshot.price),
                exit_reason=exit_reason,
            )
            return TradeDecision(EXIT, exit_reason, EXITED, exited)

        breakeven_reason = self._breakeven_move_reason(updated, snapshot)
        if breakeven_reason is not None:
            next_state = TRAILING_PROFIT if updated.state == TRAILING_PROFIT else PROTECT_BREAKEVEN
            moved = replace(
                updated,
                state=next_state,
                current_sl=self._breakeven_stop(updated),
            )
            return TradeDecision(MOVE_SL_TO_BREAKEVEN, breakeven_reason, next_state, moved)

        hold_state = updated.state if updated.state in {ENTERED, PROTECT_BREAKEVEN, TRAILING_PROFIT} else ENTERED
        held = replace(updated, state=hold_state)
        return TradeDecision(HOLD, "hold_position", hold_state, held)

    def _long_entry_blockers(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> list[str]:
        blockers: list[str] = []
        if setup.side != BUY:
            blockers.append("entry_blocked_side_not_buy")
        if setup.score < self.min_long_score:
            blockers.append("entry_blocked_low_score")
        if setup.btc_regime in {BTC_BEARISH, BTC_DUMP_RISK}:
            blockers.append("entry_blocked_btc_regime")
        if snapshot.spread_bps > self.max_spread_bps:
            blockers.append("entry_blocked_spread")
        if snapshot.buy_flow <= snapshot.sell_flow * self.flow_ratio:
            blockers.append("entry_blocked_buy_flow")
        if snapshot.volume_impulse < self.min_entry_volume_impulse:
            blockers.append("entry_blocked_volume_impulse")
        if snapshot.ask_wall_strength > self.ask_wall_entry_limit:
            blockers.append("entry_blocked_ask_wall")
        if snapshot.support is not None and snapshot.price < snapshot.support:
            blockers.append("entry_blocked_below_support")
        if snapshot.ema20 is not None and snapshot.price < snapshot.ema20:
            blockers.append("entry_blocked_below_ema20")
        if snapshot.vwap is not None and snapshot.price < snapshot.vwap:
            blockers.append("entry_blocked_below_vwap")
        return blockers

    def _short_entry_blockers(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> list[str]:
        blockers: list[str] = []

        if setup.side != SELL:
            blockers.append("entry_blocked_side_not_sell")

        if setup.score < self.min_long_score:
            blockers.append("entry_blocked_low_score")

        if setup.btc_regime not in {BTC_BEARISH, BTC_DUMP_RISK}:
            blockers.append("entry_blocked_btc_regime")

        if snapshot.spread_bps > self.max_spread_bps:
            blockers.append("entry_blocked_spread")

        if snapshot.sell_flow <= snapshot.buy_flow * self.flow_ratio:
            blockers.append("entry_blocked_sell_flow")

        if snapshot.volume_impulse < self.min_entry_volume_impulse:
            blockers.append("entry_blocked_volume_impulse")

        if snapshot.bid_wall_strength > self.bid_wall_entry_limit:
            blockers.append("entry_blocked_bid_wall")

        if snapshot.resistance is not None and snapshot.price > snapshot.resistance:
            blockers.append("entry_blocked_above_resistance")

        if snapshot.ema20 is not None and snapshot.price > snapshot.ema20:
            blockers.append("entry_blocked_above_ema20")

        if snapshot.vwap is not None and snapshot.price > snapshot.vwap:
            blockers.append("entry_blocked_above_vwap")

        return blockers

    def _initial_stop_loss(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> float:
        if setup.side == BUY:
            support_sl = setup.stop_loss
            if snapshot.support is not None:
                support_sl = snapshot.support * self.support_buffer_multiplier
            return min(setup.stop_loss, support_sl)

        if setup.side == SELL:
            resistance_sl = setup.stop_loss
            if snapshot.resistance is not None:
                resistance_sl = snapshot.resistance * self.resistance_buffer_multiplier
            return max(setup.stop_loss, resistance_sl)

        raise ValueError(f"unsupported side: {setup.side}")

    def _calculate_initial_risk(self, side: str, entry_price: float, stop_loss: float) -> float:
        if side == BUY:
            risk = entry_price - stop_loss
        elif side == SELL:
            risk = stop_loss - entry_price
        else:
            raise ValueError(f"unsupported side: {side}")

        if risk <= 0:
            raise ValueError("initial risk must be positive")

        return risk

    def _refresh_position_metrics(self, position: TradePosition, snapshot: OrderflowSnapshot) -> TradePosition:
        price = float(snapshot.price)
        max_price = max(position.max_price, price)
        min_price = min(position.min_price, price)
        bars_in_trade = position.bars_in_trade + 1

        if position.side == BUY:
            gain_r = (max_price - position.entry_price) / position.initial_risk
            drawdown_r = (position.entry_price - min_price) / position.initial_risk
        elif position.side == SELL:
            gain_r = (position.entry_price - min_price) / position.initial_risk
            drawdown_r = (max_price - position.entry_price) / position.initial_risk
        else:
            raise ValueError(f"unsupported side: {position.side}")

        return replace(
            position,
            max_price=max_price,
            min_price=min_price,
            max_gain_r=max(position.max_gain_r, gain_r),
            max_drawdown_r=max(position.max_drawdown_r, drawdown_r),
            bars_in_trade=bars_in_trade,
        )

    def _apply_management_v2(
        self, position: TradePosition, snapshot: OrderflowSnapshot
    ) -> tuple[TradePosition, Optional[str]]:
        if self.management_policy != MANAGEMENT_POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R:
            return position, None

        if position.max_gain_r < 1.0:
            return position, None

        protected = self._protect_after_1r_stop(position) if self.protect_after_1r else position
        current_r = self._current_unrealized_r(
            protected.side,
            protected.entry_price,
            float(snapshot.price),
            protected.initial_risk,
        )
        trailing_floor_r = protected.max_gain_r * 0.60
        protected_floor_r = max(trailing_floor_r, self.min_protected_r_after_1r)

        if current_r <= protected_floor_r:
            return replace(protected, state=TRAILING_PROFIT), EXIT_TRAILING_40PCT_GIVEBACK_AFTER_1R

        return replace(protected, state=TRAILING_PROFIT), None

    def _protect_after_1r_stop(self, position: TradePosition) -> TradePosition:
        if position.side == BUY:
            protected_sl = position.entry_price + self.min_protected_r_after_1r * position.initial_risk
            return replace(position, current_sl=max(position.current_sl, protected_sl))

        if position.side == SELL:
            protected_sl = position.entry_price - self.min_protected_r_after_1r * position.initial_risk
            return replace(position, current_sl=min(position.current_sl, protected_sl))

        raise ValueError(f"unsupported side: {position.side}")

    @staticmethod
    def _current_unrealized_r(side: str, entry_price: float, price: float, initial_risk: float) -> float:
        if initial_risk <= 0:
            raise ValueError("initial risk must be positive")
        if side == BUY:
            return (price - entry_price) / initial_risk
        if side == SELL:
            return (entry_price - price) / initial_risk
        raise ValueError(f"unsupported side: {side}")

    def _breakeven_move_reason(self, position: TradePosition, snapshot: OrderflowSnapshot) -> Optional[str]:
        if position.state in {PROTECT_BREAKEVEN, TRAILING_PROFIT, EXITED}:
            return None

        if self._should_move_to_breakeven(position, snapshot):
            return "sl_moved_to_breakeven"

        if position.state == ENTERED and position.max_gain_r >= 1.0:
            if position.side in {BUY, SELL}:
                return "sl_moved_to_breakeven_after_max_r"
            raise ValueError(f"unsupported side: {position.side}")

        return None

    def _should_move_to_breakeven(self, position: TradePosition, snapshot: OrderflowSnapshot) -> bool:
        price = float(snapshot.price)
        target = 0.5 * position.initial_risk

        if position.side == BUY:
            return (
                price >= position.entry_price + target
                and snapshot.buy_flow >= snapshot.sell_flow
                and snapshot.volume_impulse >= self.min_breakeven_volume_impulse
                and price >= position.entry_price
            )

        if position.side == SELL:
            return (
                price <= position.entry_price - target
                and snapshot.sell_flow >= snapshot.buy_flow
                and snapshot.volume_impulse >= self.min_breakeven_volume_impulse
                and price <= position.entry_price
            )

        raise ValueError(f"unsupported side: {position.side}")

    def _breakeven_stop(self, position: TradePosition) -> float:
        if position.side == BUY:
            return max(position.current_sl, position.entry_price * self.buy_fee_buffer_multiplier)
        if position.side == SELL:
            return min(position.current_sl, position.entry_price * self.sell_fee_buffer_multiplier)
        raise ValueError(f"unsupported side: {position.side}")

    def _exit_reason(self, position: TradePosition, snapshot: OrderflowSnapshot) -> Optional[str]:
        bars_since_entry = snapshot.bars_since_entry if snapshot.bars_since_entry is not None else position.bars_in_trade
        price = float(snapshot.price)

        if position.side == BUY:
            if price <= position.current_sl:
                return "exit_stop_loss_hit"
            if snapshot.sell_flow > snapshot.buy_flow * self.strong_reversal_ratio:
                return "exit_sell_flow_dominance"
            if snapshot.ask_wall_strength >= self.strong_wall_exit_threshold and snapshot.buy_flow < snapshot.sell_flow:
                return "exit_ask_wall_pressure"
            if snapshot.support is not None and price < snapshot.support and bars_since_entry >= 2:
                return "exit_lost_support"
            if snapshot.ema20 is not None and price < snapshot.ema20 and snapshot.sell_flow > snapshot.buy_flow:
                return "exit_below_ema20_with_selling"
            return None

        if position.side == SELL:
            if price >= position.current_sl:
                return "exit_stop_loss_hit"
            if snapshot.buy_flow > snapshot.sell_flow * self.strong_reversal_ratio:
                return "exit_buy_flow_dominance"
            if snapshot.bid_wall_strength >= self.strong_wall_exit_threshold and snapshot.sell_flow < snapshot.buy_flow:
                return "exit_bid_wall_pressure"
            if snapshot.resistance is not None and price > snapshot.resistance and bars_since_entry >= 2:
                return "exit_lost_resistance"
            if snapshot.ema20 is not None and price > snapshot.ema20 and snapshot.buy_flow > snapshot.sell_flow:
                return "exit_above_ema20_with_buying"
            return None

        raise ValueError(f"unsupported side: {position.side}")
