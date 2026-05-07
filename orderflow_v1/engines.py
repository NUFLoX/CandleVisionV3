from __future__ import annotations

import logging
import time

from .bookflow import SymbolFlowState
from .config import Settings
from .indicators import add_indicators, local_resistance, local_support
from .models import Signal

logger = logging.getLogger("OrderFlow.Engines")


def _bps_distance(price_a: float, price_b: float) -> float:
    if not price_a or not price_b:
        return 10**9
    return abs(price_a - price_b) / price_b * 10000.0


def _sum_trade_notional(state: SymbolFlowState, side: str) -> float:
    return sum(trade.notional for trade in state.trades if trade.side == side)


def _signed_delta_notional(state: SymbolFlowState) -> float:
    buy = _sum_trade_notional(state, "Buy")
    sell = _sum_trade_notional(state, "Sell")
    return buy - sell


def _top_imbalance(state: SymbolFlowState, depth: int) -> float:
    bids, asks = state.top_book(depth)
    bid_vol = sum(size for _, size in bids)
    ask_vol = sum(size for _, size in asks)
    denom = bid_vol + ask_vol
    if denom == 0:
        return 0.0
    return (bid_vol - ask_vol) / denom


def _best_bid_ask(state: SymbolFlowState) -> tuple[float | None, float | None]:
    if not state.bids or not state.asks:
        return None, None
    return max(state.bids), min(state.asks)


def _spread_bps(best_bid: float, best_ask: float) -> float:
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return 10**9
    return (best_ask - best_bid) / mid * 10000.0


def _wall_persistence_ratio(state: SymbolFlowState, near_price: float, tolerance_bps: float, side: str) -> float:
    events = state.bid_walls if side == "bid" else state.ask_walls
    if not events:
        return 0.0
    hits = [event for event in events if _bps_distance(event.price, near_price) <= tolerance_bps]
    if not hits:
        return 0.0
    return len(hits) / max(len(events), 1)


class RealtimeOrderFlowEngine:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._last_emitted: dict[str, float] = {}

    def analyze(self, symbol: str, df, state: SymbolFlowState | None) -> list[Signal]:
        if df.empty or state is None:
            return []
        if time.time() - state.last_update_ts > self.settings.stale_book_seconds:
            return []

        df = add_indicators(df)
        last = df.iloc[-1]
        support = local_support(df.iloc[:-1], self.settings.support_lookback_bars)
        resistance = local_resistance(df.iloc[:-1], self.settings.support_lookback_bars)
        if support is None or resistance is None:
            return []

        best_bid, best_ask = _best_bid_ask(state)
        if best_bid is None or best_ask is None:
            return []

        spread_bps = _spread_bps(best_bid, best_ask)
        if spread_bps > self.settings.max_spread_bps:
            return []

        mid = state.last_mid or float(last["close"])
        atr = max(float(last["atr_14"]), mid * 0.0025)
        top_imbalance = _top_imbalance(state, self.settings.book_depth)
        delta_notional = _signed_delta_notional(state)
        buy_notional = _sum_trade_notional(state, "Buy")
        sell_notional = _sum_trade_notional(state, "Sell")
        tape_total_notional = buy_notional + sell_notional
        support_dist = _bps_distance(mid, support)
        resistance_dist = _bps_distance(mid, resistance)
        bid_persistence = _wall_persistence_ratio(state, support, self.settings.support_tolerance_bps, "bid")
        ask_persistence = _wall_persistence_ratio(state, resistance, self.settings.resistance_tolerance_bps, "ask")

        if tape_total_notional < self.settings.min_realtime_trade_notional:
            return []

        signals: list[Signal] = []

        falling_now = float(last["close"]) < float(last["open"])
        rising_now = float(last["close"]) > float(last["open"])

        absorption_long_score = 0.0
        reasons: list[str] = []
        if support_dist <= self.settings.support_tolerance_bps:
            absorption_long_score += 1.2
            reasons.append(f"price_near_support={support_dist:.1f}bps")
        if top_imbalance >= self.settings.min_book_imbalance_abs:
            absorption_long_score += min(top_imbalance * 6.0, 1.8)
            reasons.append(f"bid_imbalance={top_imbalance:.2f}")
        if bid_persistence >= self.settings.min_wall_persistence:
            absorption_long_score += min(bid_persistence * 3.2, 1.4)
            reasons.append(f"bid_wall_persist={bid_persistence:.2f}")
        if sell_notional > buy_notional * 1.12 and float(last["close_pos"]) > 0.52:
            absorption_long_score += 1.3
            reasons.append("sells_absorbed_near_lows")
        if float(last["return_3"]) < 0 and delta_notional >= self.settings.min_delta_abs:
            absorption_long_score += 1.4
            reasons.append("delta_positive_while_price_falls")
        if float(last["close"]) > float(last["ema_20"]):
            absorption_long_score += 0.6
            reasons.append("close_above_ema20")
        if falling_now and float(last["range_pct"]) < 1.8:
            absorption_long_score += 0.3
        if support_dist > self.settings.support_tolerance_bps * 1.2:
            absorption_long_score = 0.0

        if absorption_long_score >= self.settings.min_absorption_score:
            entry = best_ask
            stop = min(support - atr * self.settings.atr_stop_mult, entry - atr * 0.8)
            risk = max(entry - stop, atr * 0.35)
            signals.append(
                Signal(
                    symbol=symbol,
                    side="Buy",
                    kind="ABSORPTION_LONG",
                    source="orderflow",
                    score=round(absorption_long_score, 2),
                    entry=entry,
                    stop_loss=stop,
                    take_profit_1=entry + risk * self.settings.tp1_rr,
                    take_profit_2=entry + risk * self.settings.tp2_rr,
                    reasons=reasons.copy(),
                    meta={
                        "support": round(support, 8),
                        "resistance": round(resistance, 8),
                        "delta_notional": round(delta_notional, 2),
                        "buy_notional": round(buy_notional, 2),
                        "sell_notional": round(sell_notional, 2),
                        "imbalance": round(top_imbalance, 4),
                        "spread_bps": round(spread_bps, 2),
                    },
                )
            )

        absorption_short_score = 0.0
        reasons = []
        if resistance_dist <= self.settings.resistance_tolerance_bps:
            absorption_short_score += 1.2
            reasons.append(f"price_near_resistance={resistance_dist:.1f}bps")
        if top_imbalance <= -self.settings.min_book_imbalance_abs:
            absorption_short_score += min(abs(top_imbalance) * 6.0, 1.8)
            reasons.append(f"ask_imbalance={top_imbalance:.2f}")
        if ask_persistence >= self.settings.min_wall_persistence:
            absorption_short_score += min(ask_persistence * 3.2, 1.4)
            reasons.append(f"ask_wall_persist={ask_persistence:.2f}")
        if buy_notional > sell_notional * 1.12 and float(last["close_pos"]) < 0.48:
            absorption_short_score += 1.3
            reasons.append("buys_absorbed_near_highs")
        if float(last["return_3"]) > 0 and delta_notional <= -self.settings.min_delta_abs:
            absorption_short_score += 1.4
            reasons.append("delta_negative_while_price_rises")
        if float(last["close"]) < float(last["ema_20"]):
            absorption_short_score += 0.6
            reasons.append("close_below_ema20")
        if rising_now and float(last["range_pct"]) < 1.8:
            absorption_short_score += 0.3
        if resistance_dist > self.settings.resistance_tolerance_bps * 1.2:
            absorption_short_score = 0.0

        if absorption_short_score >= self.settings.min_absorption_score:
            entry = best_bid
            stop = max(resistance + atr * self.settings.atr_stop_mult, entry + atr * 0.8)
            risk = max(stop - entry, atr * 0.35)
            signals.append(
                Signal(
                    symbol=symbol,
                    side="Sell",
                    kind="ABSORPTION_SHORT",
                    source="orderflow",
                    score=round(absorption_short_score, 2),
                    entry=entry,
                    stop_loss=stop,
                    take_profit_1=entry - risk * self.settings.tp1_rr,
                    take_profit_2=entry - risk * self.settings.tp2_rr,
                    reasons=reasons.copy(),
                    meta={
                        "support": round(support, 8),
                        "resistance": round(resistance, 8),
                        "delta_notional": round(delta_notional, 2),
                        "buy_notional": round(buy_notional, 2),
                        "sell_notional": round(sell_notional, 2),
                        "imbalance": round(top_imbalance, 4),
                        "spread_bps": round(spread_bps, 2),
                    },
                )
            )

        breakout_long_score = 0.0
        reasons = []
        if best_ask > resistance:
            breakout_long_score += 1.6
            reasons.append("trade_above_resistance")
        if top_imbalance >= max(self.settings.min_book_imbalance_abs, 0.16):
            breakout_long_score += min(top_imbalance * 5.6, 1.6)
            reasons.append(f"bid_imbalance={top_imbalance:.2f}")
        if buy_notional > sell_notional * 1.25 and delta_notional >= self.settings.min_delta_abs:
            breakout_long_score += 1.2
            reasons.append("aggressive_buys_dominate")
        if float(last["turnover"]) >= float(last["volume_ma_20"]) * self.settings.min_breakout_turnover_multiple:
            breakout_long_score += 0.9
            reasons.append("turnover_expansion")
        if float(last["close"]) > float(last["ema_20"]) > float(last["ema_50"]):
            breakout_long_score += 0.7
            reasons.append("trend_alignment")

        if breakout_long_score >= self.settings.min_breakout_score:
            entry = best_ask
            stop = max(resistance - atr * self.settings.atr_stop_mult, entry - atr * 1.1)
            risk = max(entry - stop, atr * 0.35)
            signals.append(
                Signal(
                    symbol=symbol,
                    side="Buy",
                    kind="BREAKOUT_LONG",
                    source="orderflow",
                    score=round(breakout_long_score, 2),
                    entry=entry,
                    stop_loss=stop,
                    take_profit_1=entry + risk * self.settings.tp1_rr,
                    take_profit_2=entry + risk * self.settings.tp2_rr,
                    reasons=reasons.copy(),
                    meta={
                        "support": round(support, 8),
                        "resistance": round(resistance, 8),
                        "imbalance": round(top_imbalance, 4),
                        "delta_notional": round(delta_notional, 2),
                        "spread_bps": round(spread_bps, 2),
                    },
                )
            )

        breakout_short_score = 0.0
        reasons = []
        if best_bid < support:
            breakout_short_score += 1.6
            reasons.append("trade_below_support")
        if top_imbalance <= -max(self.settings.min_book_imbalance_abs, 0.16):
            breakout_short_score += min(abs(top_imbalance) * 5.6, 1.6)
            reasons.append(f"ask_imbalance={top_imbalance:.2f}")
        if sell_notional > buy_notional * 1.25 and delta_notional <= -self.settings.min_delta_abs:
            breakout_short_score += 1.2
            reasons.append("aggressive_sells_dominate")
        if float(last["turnover"]) >= float(last["volume_ma_20"]) * self.settings.min_breakout_turnover_multiple:
            breakout_short_score += 0.9
            reasons.append("turnover_expansion")
        if float(last["close"]) < float(last["ema_20"]) < float(last["ema_50"]):
            breakout_short_score += 0.7
            reasons.append("trend_alignment")

        if breakout_short_score >= self.settings.min_breakout_score:
            entry = best_bid
            stop = min(support + atr * self.settings.atr_stop_mult, entry + atr * 1.1)
            risk = max(stop - entry, atr * 0.35)
            signals.append(
                Signal(
                    symbol=symbol,
                    side="Sell",
                    kind="BREAKOUT_SHORT",
                    source="orderflow",
                    score=round(breakout_short_score, 2),
                    entry=entry,
                    stop_loss=stop,
                    take_profit_1=entry - risk * self.settings.tp1_rr,
                    take_profit_2=entry - risk * self.settings.tp2_rr,
                    reasons=reasons.copy(),
                    meta={
                        "support": round(support, 8),
                        "resistance": round(resistance, 8),
                        "imbalance": round(top_imbalance, 4),
                        "delta_notional": round(delta_notional, 2),
                        "spread_bps": round(spread_bps, 2),
                    },
                )
            )

        return [signal for signal in signals if self._allow_emit(signal)]

    def _allow_emit(self, signal: Signal) -> bool:
        now = time.time()
        key = signal.dedupe_key()
        last_ts = self._last_emitted.get(key, 0.0)
        if now - last_ts < self.settings.signal_cooldown_seconds:
            return False
        self._last_emitted[key] = now
        return True


class MacroFlowEngine:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _turnover_threshold(self, tf_name: str) -> float:
        return {
            "60": self.settings.macro_min_turnover_60,
            "240": self.settings.macro_min_turnover_240,
            "D": self.settings.macro_min_turnover_d,
        }.get(tf_name, self.settings.macro_min_turnover_60)

    def _corridor_threshold(self, tf_name: str) -> float:
        return {
            "60": self.settings.macro_max_corridor_pct_60,
            "240": self.settings.macro_max_corridor_pct_240,
            "D": self.settings.macro_max_corridor_pct_d,
        }.get(tf_name, self.settings.macro_max_corridor_pct_60)

    def analyze(self, symbol: str, tf_map: dict[str, any]) -> Signal | None:
        best = None
        for tf_name, df in tf_map.items():
            if df.empty or len(df) < 25:
                continue

            df = add_indicators(df)
            last = df.iloc[-1]
            window = df.tail(24 if tf_name == "60" else min(len(df), 30))
            corridor_high = float(window["high"].max())
            corridor_low = float(window["low"].min())
            corridor_pct = ((corridor_high - corridor_low) / max(corridor_low, 1e-12)) * 100.0
            turnover_sum = float(window["turnover"].sum())
            turnover_avg = float(window["turnover"].mean())
            volume_expansion = float(last["turnover"]) / max(turnover_avg, 1.0)
            close_to_high = (corridor_high - float(last["close"])) / max(float(last["close"]), 1e-12) * 100.0
            slope_ok = float(last["ema_20"]) >= float(last["ema_50"]) * 1.000
            last_green = float(last["close"]) >= float(last["open"])
            close_pos = float(last["close_pos"])

            if self.settings.macro_require_breakout_proximity and close_to_high > self.settings.macro_max_close_to_breakout_pct:
                continue
            if volume_expansion < self.settings.macro_min_volume_expansion:
                continue
            if turnover_sum < self._turnover_threshold(tf_name):
                continue
            if corridor_pct > self._corridor_threshold(tf_name):
                continue
            if self.settings.macro_require_trend_alignment and not slope_ok:
                continue

            score = 0.0
            reasons: list[str] = []

            score += 1.0
            reasons.append(f"compression={corridor_pct:.2f}%")

            score += 1.2
            reasons.append(f"turnover_sum={turnover_sum/1_000_000:.1f}m")

            score += 1.0
            reasons.append(f"close_to_breakout={close_to_high:.2f}%")

            if volume_expansion >= max(self.settings.macro_min_volume_expansion, 2.0):
                score += 1.0
            else:
                score += 0.7
            reasons.append(f"volume_expansion={volume_expansion:.2f}x")

            if slope_ok:
                score += 0.5
                reasons.append("ema20_above_ema50")
            if last_green:
                score += 0.2
                reasons.append("bullish_close")
            if close_pos >= 0.65:
                score += 0.3
                reasons.append(f"close_pos={close_pos:.2f}")

            if score < self.settings.min_macro_score:
                continue

            entry = float(last["close"])
            atr = max(float(last["atr_14"]), entry * 0.015)
            stop = entry - atr * 1.4
            risk = max(entry - stop, atr * 0.35)
            candidate = Signal(
                symbol=symbol,
                side="Buy",
                kind="MACRO_ACCUMULATION",
                source="macro",
                score=round(score, 2),
                entry=entry,
                stop_loss=stop,
                take_profit_1=entry + risk * self.settings.tp1_rr,
                take_profit_2=entry + risk * self.settings.tp2_rr,
                reasons=[f"tf={tf_name}"] + reasons,
                meta={
                    "tf": tf_name,
                    "corridor_high": round(corridor_high, 8),
                    "corridor_low": round(corridor_low, 8),
                    "turnover_sum": round(turnover_sum, 2),
                    "close_to_breakout_pct": round(close_to_high, 4),
                    "volume_expansion": round(volume_expansion, 4),
                },
            )
            if best is None or candidate.score > best.score:
                best = candidate
        return best
