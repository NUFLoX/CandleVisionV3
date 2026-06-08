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
BTC_BULLISH = "BTC_BULLISH"
BTC_NEUTRAL = "BTC_NEUTRAL"
RISK_OFF = "RISK_OFF"
RISK_ON = "RISK_ON"
ABSORPTION_ZONE = "ABSORPTION_ZONE"
PRE_IMPULSE_ZONE = "PRE_IMPULSE_ZONE"
BREAKOUT_PRESSURE = "BREAKOUT_PRESSURE"
ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION = "entry_blocked_absorption_weak_confirmation"

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
    signal_kind: str = ""
    market_regime: Optional[str] = None


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
        absorption_flow_ratio: float = 1.15,
        trade_executor_mode: str = "paper",
        testnet_volume_impulse_relaxation: float = 0.85,
        testnet_absorption_flow_ratio: float = 1.05,
        testnet_pre_impulse_bullish_flow_ratio: float = 1.05,
        testnet_risk_off_volume_impulse_relaxation: float = 0.75,
    ) -> None:
        self.absorption_flow_ratio = max(float(absorption_flow_ratio), 1.0)
        self.trade_executor_mode = str(trade_executor_mode or "paper").strip().lower()
        self.testnet_volume_impulse_relaxation = min(max(float(testnet_volume_impulse_relaxation), 0.0), 1.0)
        self.testnet_absorption_flow_ratio = max(float(testnet_absorption_flow_ratio), 1.0)
        self.testnet_pre_impulse_bullish_flow_ratio = max(float(testnet_pre_impulse_bullish_flow_ratio), 1.0)
        self.testnet_risk_off_volume_impulse_relaxation = min(
            max(float(testnet_risk_off_volume_impulse_relaxation), 0.0), 1.0
        )
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
            if self._is_absorption_setup(setup) and not self._absorption_long_gate_passed(setup, snapshot):
                return TradeDecision(WATCH, ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION, WATCH_ENTRY, None)
            blockers = self._long_entry_blockers(setup, snapshot)
            if blockers:
                return TradeDecision(WATCH, blockers[0], WATCH_ENTRY, None)
            return TradeDecision(ENTER_LONG, "entry_allowed_long", ENTERED, None)

        if setup.side == SELL:
            if self._is_absorption_setup(setup) and not self._absorption_short_gate_passed(setup, snapshot):
                return TradeDecision(WATCH, ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION, WATCH_ENTRY, None)
            blockers = self._short_entry_blockers(setup, snapshot)
            if blockers:
                return TradeDecision(WATCH, blockers[0], WATCH_ENTRY, None)
            return TradeDecision(ENTER_SHORT, "entry_allowed_short", ENTERED, None)

        return TradeDecision(WATCH, "entry_blocked_unknown_side", WATCH_ENTRY, None)

    def absorption_gate_diagnostics(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> dict[str, object]:
        gate_passed = True
        if setup.side == BUY:
            gate_passed = self._absorption_long_gate_passed(setup, snapshot)
        elif setup.side == SELL:
            gate_passed = self._absorption_short_gate_passed(setup, snapshot)

        diagnostics = {
            "absorption_strict_gate": True,
            "absorption_gate_passed": gate_passed,
            "absorption_gate_reason": None if gate_passed else ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION,
            "btc_regime": setup.btc_regime,
            "market_regime": setup.market_regime,
            "buy_flow": float(snapshot.buy_flow),
            "sell_flow": float(snapshot.sell_flow),
            "volume_impulse": float(snapshot.volume_impulse),
            "required_volume_impulse": float(self.min_entry_volume_impulse),
            "spread_bps": float(snapshot.spread_bps),
            "ask_wall_strength": float(snapshot.ask_wall_strength),
            "bid_wall_strength": float(snapshot.bid_wall_strength),
            "support": snapshot.support,
            "resistance": snapshot.resistance,
        }
        diagnostics.update(self.entry_gate_diagnostics(setup, snapshot))
        return diagnostics

    def entry_gate_diagnostics(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> dict[str, object]:
        volume_ratio_to_required = None
        if self.min_entry_volume_impulse > 0:
            volume_ratio_to_required = float(snapshot.volume_impulse) / float(self.min_entry_volume_impulse)
        volume_relaxed = self._volume_impulse_passed(snapshot, relaxed=True) and not self._volume_impulse_passed(snapshot)
        buy_flow_relaxed = self._buy_flow_passed(setup, snapshot, relaxed=True) and not self._buy_flow_passed(setup, snapshot)
        risk_off_exception = self._testnet_risk_off_exception_passed(setup, snapshot)
        testnet_entry_gate_relaxed = self._is_testnet_mode() and (volume_relaxed or buy_flow_relaxed)
        testnet_entry_gate_relaxed = testnet_entry_gate_relaxed or risk_off_exception
        if self._is_testnet_mode() and self._is_absorption_setup(setup) and setup.side == BUY:
            confirmations = self._testnet_absorption_long_confirmations(snapshot)
            testnet_entry_gate_relaxed = testnet_entry_gate_relaxed or (sum(confirmations.values()) >= 2 and not self._strict_absorption_long_confirmations_passed(snapshot))
        return {
            "trade_executor_mode": self.trade_executor_mode,
            "executor_management_policy": self.management_policy,
            "testnet_entry_gate_relaxed": bool(testnet_entry_gate_relaxed),
            "testnet_risk_off_exception": bool(risk_off_exception),
            "testnet_relaxation_reason": "strong_testnet_entry_during_risk_off" if risk_off_exception else None,
            "volume_impulse_relaxed_for_testnet": bool(volume_relaxed),
            "volume_impulse_ratio_to_required": volume_ratio_to_required,
            "buy_flow_relaxed_for_testnet": bool(buy_flow_relaxed),
            "signal_kind": str(setup.signal_kind or ""),
            "btc_regime": setup.btc_regime,
            "market_regime": setup.market_regime,
            "buy_flow": float(snapshot.buy_flow),
            "sell_flow": float(snapshot.sell_flow),
            "volume_impulse": float(snapshot.volume_impulse),
            "required_volume_impulse": float(self.min_entry_volume_impulse),
            "ask_wall_strength": float(snapshot.ask_wall_strength),
            "spread_bps": float(snapshot.spread_bps),
        }

    def _is_absorption_setup(self, setup: TradeSetup) -> bool:
        if str(setup.signal_kind or "").upper() == ABSORPTION_ZONE:
            return True
        return any(str(reason).upper() == ABSORPTION_ZONE for reason in (setup.reasons or []))

    def _absorption_long_gate_passed(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> bool:
        if setup.btc_regime in {BTC_BEARISH, BTC_DUMP_RISK}:
            return False
        if self._is_risk_off_regime(setup.market_regime):
            return False
        if snapshot.spread_bps > self.max_spread_bps:
            return False
        if snapshot.support is not None and snapshot.price < snapshot.support:
            return False
        if setup.entry_hint > 0 and snapshot.price < setup.entry_hint:
            return False
        if self._is_testnet_mode():
            return sum(self._testnet_absorption_long_confirmations(snapshot).values()) >= 2
        return self._strict_absorption_long_confirmations_passed(snapshot)

    def _absorption_short_gate_passed(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> bool:
        market_regime = str(setup.market_regime or "").upper()
        if setup.btc_regime == BTC_BULLISH:
            return False
        if market_regime == RISK_ON:
            return False
        if snapshot.sell_flow < snapshot.buy_flow * self.absorption_flow_ratio:
            return False
        if snapshot.volume_impulse < self.min_entry_volume_impulse:
            return False
        if snapshot.spread_bps > self.max_spread_bps:
            return False
        if snapshot.bid_wall_strength > self.bid_wall_entry_limit:
            return False
        if snapshot.resistance is not None and snapshot.price > snapshot.resistance:
            return False
        if setup.entry_hint > 0 and snapshot.price > setup.entry_hint:
            return False
        return True

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
        btc_dangerous = setup.btc_regime in {BTC_BEARISH, BTC_DUMP_RISK}
        if btc_dangerous:
            blockers.append("entry_blocked_market_regime")

        market_risk_off = self._is_risk_off_regime(setup.market_regime)
        testnet_risk_off_candidate = self._testnet_risk_off_exception_candidate(setup)
        if market_risk_off and not btc_dangerous and not testnet_risk_off_candidate:
            blockers.append("entry_blocked_market_regime")

        if setup.score < self.min_long_score:
            blockers.append("entry_blocked_low_score")
        if snapshot.spread_bps > self.max_spread_bps:
            blockers.append("entry_blocked_spread")
        absorption_testnet_relaxed = self._is_testnet_mode() and self._is_absorption_setup(setup)
        if testnet_risk_off_candidate:
            if not self._testnet_risk_off_buy_flow_passed(snapshot):
                blockers.append("entry_blocked_buy_flow")
            if not self._testnet_risk_off_volume_impulse_passed(snapshot):
                blockers.append("entry_blocked_volume_impulse")
            if snapshot.ask_wall_strength > self.ask_wall_entry_limit:
                blockers.append("entry_blocked_ask_wall")
        else:
            if not absorption_testnet_relaxed and not self._buy_flow_passed(setup, snapshot, relaxed=True):
                blockers.append("entry_blocked_buy_flow")
            if not absorption_testnet_relaxed and not self._volume_impulse_passed(snapshot, relaxed=True):
                blockers.append("entry_blocked_volume_impulse")
            if not absorption_testnet_relaxed and snapshot.ask_wall_strength > self.ask_wall_entry_limit:
                blockers.append("entry_blocked_ask_wall")
        if snapshot.support is not None and snapshot.price < snapshot.support:
            blockers.append("entry_blocked_below_support")
        if snapshot.ema20 is not None and snapshot.price < snapshot.ema20:
            blockers.append("entry_blocked_below_ema20")
        if snapshot.vwap is not None and snapshot.price < snapshot.vwap:
            blockers.append("entry_blocked_below_vwap")
        return blockers

    def _is_testnet_mode(self) -> bool:
        return self.trade_executor_mode == "testnet"

    @staticmethod
    def _normalize_regime(value: Optional[str]) -> str:
        return str(value or "").strip().upper().replace("-", "_")

    def _is_risk_off_regime(self, value: Optional[str]) -> bool:
        return self._normalize_regime(value) == RISK_OFF

    def _is_breakout_pressure_setup(self, setup: TradeSetup) -> bool:
        if str(setup.signal_kind or "").upper() == BREAKOUT_PRESSURE:
            return True
        return any(str(reason).upper() == BREAKOUT_PRESSURE for reason in (setup.reasons or []))

    def _is_strong_testnet_risk_off_signal(self, setup: TradeSetup) -> bool:
        return self._is_pre_impulse_setup(setup) or self._is_breakout_pressure_setup(setup)

    def _testnet_risk_off_exception_candidate(self, setup: TradeSetup) -> bool:
        return (
            self._is_testnet_mode()
            and setup.side == BUY
            and self._is_risk_off_regime(setup.market_regime)
            and setup.btc_regime in {BTC_BULLISH, BTC_NEUTRAL}
            and self._is_strong_testnet_risk_off_signal(setup)
        )

    def _testnet_risk_off_required_volume_impulse(self) -> float:
        return self.min_entry_volume_impulse * self.testnet_risk_off_volume_impulse_relaxation

    def _testnet_risk_off_buy_flow_passed(self, snapshot: OrderflowSnapshot) -> bool:
        return snapshot.buy_flow > snapshot.sell_flow

    def _testnet_risk_off_volume_impulse_passed(self, snapshot: OrderflowSnapshot) -> bool:
        return snapshot.volume_impulse >= self._testnet_risk_off_required_volume_impulse()

    def _testnet_risk_off_exception_passed(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> bool:
        return (
            self._testnet_risk_off_exception_candidate(setup)
            and self._testnet_risk_off_buy_flow_passed(snapshot)
            and self._testnet_risk_off_volume_impulse_passed(snapshot)
            and snapshot.ask_wall_strength <= self.ask_wall_entry_limit
            and snapshot.spread_bps <= self.max_spread_bps
        )

    def _is_pre_impulse_setup(self, setup: TradeSetup) -> bool:
        if str(setup.signal_kind or "").upper() == PRE_IMPULSE_ZONE:
            return True
        return any(str(reason).upper() == PRE_IMPULSE_ZONE for reason in (setup.reasons or []))

    def _required_volume_impulse(self, *, relaxed: bool = False) -> float:
        if relaxed and self._is_testnet_mode():
            return self.min_entry_volume_impulse * self.testnet_volume_impulse_relaxation
        return self.min_entry_volume_impulse

    def _volume_impulse_passed(self, snapshot: OrderflowSnapshot, *, relaxed: bool = False) -> bool:
        return snapshot.volume_impulse >= self._required_volume_impulse(relaxed=relaxed)

    def _buy_flow_ratio(self, setup: TradeSetup, *, relaxed: bool = False) -> float:
        if (
            relaxed
            and self._is_testnet_mode()
            and setup.btc_regime == BTC_BULLISH
            and self._is_pre_impulse_setup(setup)
        ):
            return min(self.flow_ratio, self.testnet_pre_impulse_bullish_flow_ratio)
        return self.flow_ratio

    def _buy_flow_passed(self, setup: TradeSetup, snapshot: OrderflowSnapshot, *, relaxed: bool = False) -> bool:
        ratio = self._buy_flow_ratio(setup, relaxed=relaxed)
        return snapshot.buy_flow > snapshot.sell_flow * ratio and snapshot.buy_flow > snapshot.sell_flow

    def _strict_absorption_long_confirmations_passed(self, snapshot: OrderflowSnapshot) -> bool:
        return (
            snapshot.buy_flow >= snapshot.sell_flow * self.absorption_flow_ratio
            and snapshot.volume_impulse >= self.min_entry_volume_impulse
            and snapshot.ask_wall_strength <= self.ask_wall_entry_limit
        )

    def _testnet_absorption_long_confirmations(self, snapshot: OrderflowSnapshot) -> dict[str, bool]:
        return {
            "buy_flow": snapshot.buy_flow >= snapshot.sell_flow * self.testnet_absorption_flow_ratio,
            "volume_impulse": snapshot.volume_impulse >= self.min_entry_volume_impulse * self.testnet_volume_impulse_relaxation,
            "ask_wall": snapshot.ask_wall_strength <= self.ask_wall_entry_limit,
        }

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

    @staticmethod
    def price_distance_r_metrics(
        *, side: str, entry_price: float, initial_risk: float, max_price: float, min_price: float
    ) -> tuple[float, float]:
        if initial_risk <= 0:
            raise ValueError("initial_risk must be positive")
        if side == BUY:
            gain_r = (max_price - entry_price) / initial_risk
            drawdown_r = (entry_price - min_price) / initial_risk
        elif side == SELL:
            gain_r = (entry_price - min_price) / initial_risk
            drawdown_r = (max_price - entry_price) / initial_risk
        else:
            raise ValueError(f"unsupported side: {side}")
        return max(gain_r, 0.0), max(drawdown_r, 0.0)

    def _refresh_position_metrics(self, position: TradePosition, snapshot: OrderflowSnapshot) -> TradePosition:
        price = float(snapshot.price)
        max_price = max(position.max_price, price)
        min_price = min(position.min_price, price)
        bars_in_trade = position.bars_in_trade + 1
        gain_r, drawdown_r = self.price_distance_r_metrics(
            side=position.side,
            entry_price=position.entry_price,
            initial_risk=position.initial_risk,
            max_price=max_price,
            min_price=min_price,
        )

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
