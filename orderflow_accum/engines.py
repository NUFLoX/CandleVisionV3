from __future__ import annotations

import logging
import math
import time

import pandas as pd

from .bookflow import SymbolFlowState
from .config import Settings
from .indicators import add_indicators, local_resistance, local_support, rolling_range_pct
from .models import Signal

logger = logging.getLogger("Accum.Engines")


def _bps_distance(price_a: float, price_b: float) -> float:
    if not price_a or not price_b:
        return 10**9
    return abs(price_a - price_b) / price_b * 10000.0


def _sum_trade_notional(state: SymbolFlowState, side: str) -> float:
    return sum(trade.notional for trade in state.trades if trade.side == side)


def _signed_delta_notional(state: SymbolFlowState) -> float:
    return _sum_trade_notional(state, "Buy") - _sum_trade_notional(state, "Sell")


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


def _wall_persistence_ratio(events, near_price: float, tolerance_bps: float) -> float:
    if not events:
        return 0.0
    hits = [event for event in events if _bps_distance(event.price, near_price) <= tolerance_bps]
    if not hits:
        return 0.0
    return len(hits) / max(len(events), 1)


def _support_hold_count(df: pd.DataFrame, support: float, tolerance_bps: float, bars: int = 12) -> int:
    if df.empty:
        return 0
    lows = pd.to_numeric(df.tail(bars)["low"], errors="coerce")
    if lows.empty:
        return 0
    return sum(1 for value in lows if _bps_distance(float(value), support) <= tolerance_bps)


def _recent_pullback_depth(df: pd.DataFrame, bars: int = 8) -> float:
    if len(df) < bars:
        return 10**9
    window = df.tail(bars)
    high = pd.to_numeric(window["high"], errors="coerce").max()
    low = pd.to_numeric(window["low"], errors="coerce").min()
    close = pd.to_numeric(window["close"], errors="coerce").iloc[-1]
    if not close or pd.isna(close):
        return 10**9
    return float((high - low) / close * 100.0)


def _apply_bps_buffer(level: float, buffer_bps: float, direction: str = "below") -> float:
    if level <= 0:
        return level
    factor = buffer_bps / 10000.0
    if direction == "below":
        return level * (1.0 - factor)
    return level * (1.0 + factor)


def _unique_sorted(values: list[float], min_gap_bps: float) -> list[float]:
    if not values:
        return []
    clean = sorted(v for v in values if v and v > 0)
    out: list[float] = []
    for value in clean:
        if not out or _bps_distance(value, out[-1]) >= min_gap_bps:
            out.append(value)
    return out


def _find_swing_high_candidates(df: pd.DataFrame, entry: float, lookback: int = 80) -> list[float]:
    if df.empty:
        return []
    highs = pd.to_numeric(df.tail(lookback)["high"], errors="coerce").reset_index(drop=True)
    candidates: list[float] = []
    for idx in range(2, len(highs) - 2):
        value = highs.iloc[idx]
        if pd.isna(value) or value <= entry:
            continue
        if value >= highs.iloc[idx - 1] and value >= highs.iloc[idx - 2] and value >= highs.iloc[idx + 1] and value >= highs.iloc[idx + 2]:
            candidates.append(float(value))
    return candidates


def _find_ask_wall_candidates(state: SymbolFlowState, entry: float, depth: int, min_gap_bps: float) -> list[float]:
    bids, asks = state.top_book(depth)
    if not asks:
        return []
    ask_sizes = [size for _, size in asks]
    avg_size = sum(ask_sizes) / max(len(ask_sizes), 1)
    threshold = max(avg_size * 1.8, 1.0)
    candidates = [float(price) for price, size in asks if price > entry and size >= threshold]
    return _unique_sorted(candidates, min_gap_bps)


def _find_structural_resistances(
    df: pd.DataFrame,
    state: SymbolFlowState | None,
    entry: float,
    fallback_risk: float,
    base_resistance: float | None,
    settings: Settings,
) -> tuple[float, float, list[str], dict[str, float]]:
    candidates: list[float] = []
    reasons: list[str] = []
    if base_resistance and base_resistance > entry:
        candidates.append(float(base_resistance))
        reasons.append("local_range_high")
    swing_candidates = _find_swing_high_candidates(df, entry, lookback=max(settings.resistance_lookback_bars, 80))
    if swing_candidates:
        candidates.extend(swing_candidates[:6])
        reasons.append("swing_highs")
    if state is not None:
        ask_candidates = _find_ask_wall_candidates(state, entry, settings.book_depth, settings.min_resistance_gap_bps)
        if ask_candidates:
            candidates.extend(ask_candidates[:6])
            reasons.append("ask_walls")

    unique = _unique_sorted(candidates, settings.min_resistance_gap_bps)

    fallback_tp1 = entry + max(fallback_risk * settings.tp1_rr, entry * 0.0035)
    fallback_tp2 = entry + max(fallback_risk * settings.tp2_rr, entry * 0.0065)
    if not unique:
        return fallback_tp1, fallback_tp2, ["rr_fallback"], {}

    r1_raw = next((value for value in unique if value > entry), None)
    if r1_raw is None:
        return fallback_tp1, fallback_tp2, ["rr_fallback"], {}

    r2_raw = next((value for value in unique if value > r1_raw and _bps_distance(value, r1_raw) >= settings.min_resistance_gap_bps), None)
    if r2_raw is None:
        extension = max((r1_raw - entry) * 1.35, fallback_risk * 1.2)
        r2_raw = r1_raw + extension

    tp1 = _apply_bps_buffer(r1_raw, settings.resistance_buffer_bps, "below")
    tp2 = _apply_bps_buffer(r2_raw, settings.second_resistance_buffer_bps, "below")
    tp1 = max(tp1, entry + entry * 0.0008)
    tp2 = max(tp2, tp1 + entry * 0.0012)

    meta = {
        "resistance_1": round(r1_raw, 8),
        "resistance_2": round(r2_raw, 8),
        "tp1_buffer_bps": round(settings.resistance_buffer_bps, 2),
        "tp2_buffer_bps": round(settings.second_resistance_buffer_bps, 2),
    }
    return tp1, tp2, reasons, meta


class MacroAccumulationEngine:
    def __init__(self, settings: Settings):
        self.settings = settings

    def analyze(self, symbol: str, frames: dict[str, pd.DataFrame]) -> Signal | None:
        best_signal: Signal | None = None
        for interval, df in frames.items():
            signal = self._analyze_interval(symbol, interval, df)
            if signal and (best_signal is None or signal.score > best_signal.score):
                best_signal = signal
        return best_signal

    def diagnose(self, symbol: str, frames: dict[str, pd.DataFrame]) -> tuple[str, float | None, dict[str, object]]:
        best_reason = "no_frames"
        best_score: float | None = None
        best_metrics: dict[str, object] = {}
        for interval, df in frames.items():
            reason, score, metrics = self._diagnose_interval(symbol, interval, df)
            if score is not None and (best_score is None or score > best_score):
                best_reason, best_score, best_metrics = reason, score, metrics
            elif best_score is None:
                best_reason, best_metrics = reason, metrics
        return best_reason, best_score, best_metrics

    def _analyze_interval(self, symbol: str, interval: str, df: pd.DataFrame) -> Signal | None:
        if df.empty or len(df) < self.settings.macro_base_lookback + 5:
            return None

        df = add_indicators(df)
        last = df.iloc[-1]
        window = df.tail(self.settings.macro_base_lookback)
        corridor_high = float(pd.to_numeric(window["high"], errors="coerce").max())
        corridor_low = float(pd.to_numeric(window["low"], errors="coerce").min())
        corridor_mid = (corridor_high + corridor_low) / 2.0
        close = float(last["close"])
        if close <= 0:
            return None

        range_pct = (corridor_high - corridor_low) / close * 100.0
        close_pos = (close - corridor_low) / max(corridor_high - corridor_low, 1e-12)
        turnover_sum = float(pd.to_numeric(window["turnover"], errors="coerce").sum())
        volume_expansion = float(last["volume_ratio"])
        atr_compression = float(last["atr_ratio_20"])
        recent_impulse_pct = float(pd.to_numeric(df.tail(6)["return_1"], errors="coerce").abs().sum() * 100.0)
        higher_lows = float(window["low"].tail(6).is_monotonic_increasing or window["low"].tail(4).iloc[-1] > window["low"].tail(8).median())

        max_range_pct = {
            "60": self.settings.macro_max_range_pct_60,
            "240": self.settings.macro_max_range_pct_240,
            "D": self.settings.macro_max_range_pct_d,
        }.get(interval, self.settings.macro_max_range_pct_240)
        min_turnover = {
            "60": self.settings.macro_min_turnover_60,
            "240": self.settings.macro_min_turnover_240,
            "D": self.settings.macro_min_turnover_d,
        }.get(interval, self.settings.macro_min_turnover_240)

        if range_pct > max_range_pct:
            return None
        if turnover_sum < min_turnover:
            return None
        if recent_impulse_pct > self.settings.macro_max_recent_impulse_pct:
            return None

        score = 0.0
        reasons: list[str] = []
        if range_pct <= max_range_pct * 0.85:
            score += 1.3
            reasons.append(f"tight_base={range_pct:.2f}%")
        if close_pos >= self.settings.effective_macro_min_close_pos:
            score += 1.0
            reasons.append(f"close_pos={close_pos:.2f}")
        if volume_expansion >= self.settings.effective_macro_min_volume_expansion:
            score += 0.8
            reasons.append(f"volume_expansion={volume_expansion:.2f}x")
        if atr_compression <= 0.92:
            score += 1.0
            reasons.append(f"atr_compression={atr_compression:.2f}")
        if float(last["ema_20"]) >= float(last["ema_50"]):
            score += 0.7
            reasons.append("ema20_above_ema50")
        if higher_lows:
            score += 0.9
            reasons.append("higher_lows_inside_base")
        close_to_high_bps = _bps_distance(close, corridor_high)
        if close_to_high_bps <= self.settings.effective_breakout_ready_bps * 1.15:
            score += 0.9
            reasons.append(f"near_range_high={close_to_high_bps:.1f}bps")
        if turnover_sum >= min_turnover * 1.8:
            score += 0.5
            reasons.append(f"turnover_sum={turnover_sum/1e6:.1f}m")

        if score < self.settings.effective_min_macro_score:
            return None

        atr = max(float(last["atr_14"]), close * 0.004)
        entry = close
        stop = min(corridor_low - atr * 0.25, entry - atr * self.settings.atr_stop_mult)
        risk = max(entry - stop, atr * 0.5)
        tp1, tp2, tp_reasons, tp_meta = _find_structural_resistances(
            df=df,
            state=None,
            entry=entry,
            fallback_risk=risk,
            base_resistance=corridor_high,
            settings=self.settings,
        )
        reward_to_r1 = (tp1 - entry) / max(entry - stop, 1e-12)
        if reward_to_r1 < self.settings.effective_min_reward_to_r1:
            return None
        reasons.extend(tp_reasons)

        reasons.append(f"mode={'EARLY' if self.settings.is_early_mode else 'CONFIRMED'}")
        return Signal(
            symbol=symbol,
            side="Buy",
            kind="BASE_BUILDUP_LONG",
            source="macro",
            score=round(score, 2),
            entry=entry,
            stop_loss=stop,
            take_profit_1=tp1,
            take_profit_2=tp2,
            reasons=reasons,
            meta={
                "tf": interval,
                "corridor_high": round(corridor_high, 8),
                "corridor_low": round(corridor_low, 8),
                "range_pct": round(range_pct, 4),
                "turnover_sum": round(turnover_sum, 2),
                "close_to_high_bps": round(close_to_high_bps, 2),
                "recent_impulse_pct": round(recent_impulse_pct, 2),
                "corridor_mid": round(corridor_mid, 8),
                "reward_to_r1": round(reward_to_r1, 3),
                "signal_mode": self.settings.signal_mode,
                **tp_meta,
            },
        )

    def _diagnose_interval(self, symbol: str, interval: str, df: pd.DataFrame) -> tuple[str, float | None, dict[str, object]]:
        if df.empty or len(df) < self.settings.macro_base_lookback + 5:
            return "not_enough_bars", None, {"tf": interval, "bars": len(df)}
        df = add_indicators(df)
        last = df.iloc[-1]
        window = df.tail(self.settings.macro_base_lookback)
        corridor_high = float(pd.to_numeric(window["high"], errors="coerce").max())
        corridor_low = float(pd.to_numeric(window["low"], errors="coerce").min())
        close = float(last["close"])
        if close <= 0:
            return "bad_close", None, {"tf": interval}
        range_pct = (corridor_high - corridor_low) / close * 100.0
        close_pos = (close - corridor_low) / max(corridor_high - corridor_low, 1e-12)
        turnover_sum = float(pd.to_numeric(window["turnover"], errors="coerce").sum())
        volume_expansion = float(last["volume_ratio"])
        atr_compression = float(last["atr_ratio_20"])
        recent_impulse_pct = float(pd.to_numeric(df.tail(6)["return_1"], errors="coerce").abs().sum() * 100.0)
        max_range_pct = {"60": self.settings.macro_max_range_pct_60, "240": self.settings.macro_max_range_pct_240, "D": self.settings.macro_max_range_pct_d}.get(interval, self.settings.macro_max_range_pct_240)
        min_turnover = {"60": self.settings.macro_min_turnover_60, "240": self.settings.macro_min_turnover_240, "D": self.settings.macro_min_turnover_d}.get(interval, self.settings.macro_min_turnover_240)
        metrics = {"tf": interval, "range_pct": round(range_pct,3), "close_pos": round(close_pos,3), "turnover_sum": round(turnover_sum,2), "volume_expansion": round(volume_expansion,3), "atr_compression": round(atr_compression,3), "recent_impulse_pct": round(recent_impulse_pct,3)}
        if range_pct > max_range_pct:
            return "range_too_wide", None, metrics
        if turnover_sum < min_turnover:
            return "turnover_too_low", None, metrics
        if recent_impulse_pct > self.settings.macro_max_recent_impulse_pct:
            return "recent_impulse_too_large", None, metrics
        score = 0.0
        if range_pct <= max_range_pct * 0.85:
            score += 1.3
        if close_pos >= self.settings.effective_macro_min_close_pos:
            score += 1.0
        if volume_expansion >= self.settings.effective_macro_min_volume_expansion:
            score += 0.8
        if atr_compression <= 0.92:
            score += 1.0
        if float(last["ema_20"]) >= float(last["ema_50"]):
            score += 0.7
        if float(window["low"].tail(6).is_monotonic_increasing or window["low"].tail(4).iloc[-1] > window["low"].tail(8).median()):
            score += 0.9
        close_to_high_bps = _bps_distance(close, corridor_high)
        if close_to_high_bps <= self.settings.effective_breakout_ready_bps * 1.15:
            score += 0.9
        if turnover_sum >= min_turnover * 1.8:
            score += 0.5
        metrics["score"] = round(score,3)
        if score < self.settings.effective_min_macro_score:
            return "macro_score_too_low", score, metrics
        atr = max(float(last["atr_14"]), close * 0.004)
        stop = min(corridor_low - atr * 0.25, close - atr * self.settings.atr_stop_mult)
        risk = max(close - stop, atr * 0.5)
        tp1, tp2, _, tp_meta = _find_structural_resistances(df=df, state=None, entry=close, fallback_risk=risk, base_resistance=corridor_high, settings=self.settings)
        reward_to_r1 = (tp1 - close) / max(close - stop, 1e-12)
        metrics.update({"reward_to_r1": round(reward_to_r1,3), **tp_meta})
        if reward_to_r1 < self.settings.effective_min_reward_to_r1:
            return "reward_to_r1_too_low", score, metrics
        return "candidate_should_signal", score, metrics


class RealtimeAccumulationEngine:
    def __init__(self, settings: Settings):
        self.settings = settings

    def analyze(self, symbol: str, df: pd.DataFrame, state: SymbolFlowState | None) -> list[Signal]:
        if df.empty or state is None:
            return []
        if time.time() - state.last_update_ts > self.settings.stale_book_seconds:
            return []

        df = add_indicators(df)
        if len(df) < 80:
            return []

        last = df.iloc[-1]
        close = float(last["close"])
        support = local_support(df.iloc[:-1], self.settings.support_lookback_bars)
        resistance = local_resistance(df.iloc[:-1], self.settings.resistance_lookback_bars)
        if support is None or resistance is None or resistance <= support:
            return []

        best_bid, best_ask = _best_bid_ask(state)
        if best_bid is None or best_ask is None:
            return []

        mid = state.last_mid or close
        spread_bps = (best_ask - best_bid) / max(mid, 1e-12) * 10000.0
        if spread_bps > self.settings.max_spread_bps:
            return []

        top_imbalance = _top_imbalance(state, self.settings.book_depth)
        buy_notional = _sum_trade_notional(state, "Buy")
        sell_notional = _sum_trade_notional(state, "Sell")
        tape_total = buy_notional + sell_notional
        delta_notional = _signed_delta_notional(state)
        if tape_total < self.settings.effective_min_trade_notional:
            return []

        support_dist = _bps_distance(mid, support)
        resistance_dist = _bps_distance(mid, resistance)
        bid_persistence = _wall_persistence_ratio(state.bid_walls, support, self.settings.support_tolerance_bps)
        ask_near_persistence = _wall_persistence_ratio(state.ask_walls, resistance, self.settings.breakout_ready_bps)
        support_holds = _support_hold_count(df, support, self.settings.support_tolerance_bps, bars=15)
        base_range_short = rolling_range_pct(df, 14)
        base_range_long = rolling_range_pct(df, 48)
        compression_ratio = base_range_short / max(base_range_long, 1e-9)
        range_duration_bars = int(min(len(df), 48))
        tf_minutes = {"1": 1, "3": 3, "5": 5, "15": 15, "30": 30, "60": 60, "120": 120, "240": 240}.get(
            str(state.interval or "1"), 1
        ) if hasattr(state, "interval") else 1
        range_duration_minutes = range_duration_bars * tf_minutes
        turnover_build = float(pd.to_numeric(df.tail(12)["turnover"], errors="coerce").sum())
        displacement_pct = abs(float(pd.to_numeric(df.tail(12)["return_1"], errors="coerce").sum() * 100.0))
        turnover_displacement_ratio = turnover_build / max(displacement_pct, 0.05)
        recent = df.tail(20).copy()
        bodies = (recent["close"] - recent["open"]).abs()
        wicks = (recent["high"] - recent["low"]).abs() - bodies
        wick_to_body_ratio = float(pd.to_numeric((wicks / (bodies + 1e-12)).clip(lower=0), errors="coerce").mean())
        range_compression_ratio = range_width_pct = (resistance - support) / max(mid, 1e-12) * 100.0
        pullback_depth_pct = _recent_pullback_depth(df, 8)

        signals: list[Signal] = []
        atr = max(float(last["atr_14"]), close * 0.0028)

        # --- Pre-Impulse Accumulation Compression detector (observe/watch phase) ---
        # Separate score from trade-ready logic: this block identifies quiet accumulation
        # before a breakout, without requiring breakout confirmation.
        range_width_pct = range_compression_ratio
        body_last_20 = float(pd.to_numeric((df.tail(20)["close"] - df.tail(20)["open"]).abs(), errors="coerce").mean())
        body_prev_20 = float(pd.to_numeric((df.tail(40).head(20)["close"] - df.tail(40).head(20)["open"]).abs(), errors="coerce").mean())
        body_compression_ratio = body_last_20 / max(body_prev_20, 1e-12)
        atr_compression = float(last.get("atr_ratio_20", 1.0))
        pre_score = 0.0
        pre_reasons: list[str] = []

        tf_scale = max(1.0, min(tf_minutes / 5.0, 6.0))
        duration_threshold_minutes = int(90 * tf_scale)
        wide_range_threshold = 5.5 + max(0.0, (tf_scale - 1.0) * 0.8)
        high_displacement_threshold = 1.3 + max(0.0, (tf_scale - 1.0) * 0.35)

        if range_width_pct <= 3.2:
            pre_score += 2.0
            pre_reasons.append(f"long_sideways_range={range_width_pct:.2f}%")
        if body_compression_ratio <= 0.7:
            pre_score += 2.0
            pre_reasons.append(f"candle_body_compression={body_compression_ratio:.2f}")
        if turnover_build > self.settings.effective_min_trade_notional * 3.0 and displacement_pct <= high_displacement_threshold:
            pre_score += 2.0
            pre_reasons.append("high_turnover_low_displacement")
        if turnover_displacement_ratio >= 2.0:
            pre_score += 1.0
            pre_reasons.append(f"turnover_displacement_ratio={turnover_displacement_ratio:.2f}")
        if sell_notional >= self.settings.effective_min_sell_pressure_notional and delta_notional > -sell_notional * 0.6:
            pre_score += 2.0
            pre_reasons.append("sell_pressure_absorbed")
        if (
            sell_notional >= self.settings.effective_min_sell_pressure_notional * 1.1
            and displacement_pct <= 1.0
            and float(last["close_pos"]) >= 0.52
        ):
            pre_score += 1.0
            pre_reasons.append("sell_pressure_absorbed_v2")
        if support_holds >= 4:
            pre_score += 1.0
            pre_reasons.append(f"support_defended={support_holds}")
        if range_duration_minutes >= duration_threshold_minutes:
            pre_score += 1.0
            pre_reasons.append(f"range_duration_minutes={range_duration_minutes}>={duration_threshold_minutes}")
        if float(last["close_pos"]) >= 0.56:
            pre_score += 1.0
            pre_reasons.append("close_in_upper_half")
        if atr_compression <= 0.92:
            pre_score += 1.0
            pre_reasons.append(f"atr_compression={atr_compression:.2f}")
        if wick_to_body_ratio >= 1.2:
            pre_score += 1.0
            pre_reasons.append(f"wick_to_body_ratio={wick_to_body_ratio:.2f}")
        if tape_total >= self.settings.effective_min_trade_notional * 1.15:
            pre_score += 1.0
            pre_reasons.append("volume_not_dead")
        if resistance_dist <= self.settings.effective_breakout_ready_bps * 1.2:
            pre_score += 2.0
            pre_reasons.append(f"price_near_range_high={resistance_dist:.1f}bps")
        if buy_notional >= sell_notional * 1.05:
            pre_score += 1.0
            pre_reasons.append("buyers_regaining_tape")
        if range_width_pct > wide_range_threshold:
            pre_score -= 2.0
            pre_reasons.append("range_too_wide")
        if tape_total < self.settings.effective_min_trade_notional * 0.8:
            pre_score -= 2.0
            pre_reasons.append("dead_volume")
        if ask_near_persistence >= 0.42:
            pre_score -= 2.0
            pre_reasons.append("heavy_ask_wall")

        pre_kind: str | None = None
        if pre_score >= 13:
            pre_kind = "BREAKOUT_PRESSURE"
        elif pre_score >= 10:
            pre_kind = "PRE_IMPULSE_ZONE"
        elif pre_score >= 7:
            pre_kind = "ABSORPTION_ZONE"
        elif pre_score >= 4:
            pre_kind = "ACCUMULATION_WATCH"

        if pre_kind:
            entry = best_ask
            stop = min(support - atr * 0.25, entry - atr * self.settings.atr_stop_mult)
            risk = max(entry - stop, atr * 0.45)
            tp1, tp2, tp_reasons, tp_meta = _find_structural_resistances(
                df=df,
                state=state,
                entry=entry,
                fallback_risk=risk,
                base_resistance=resistance,
                settings=self.settings,
            )
            pre_final_reasons = pre_reasons + tp_reasons
            signals.append(
                Signal(
                    symbol=symbol,
                    side="Buy",
                    kind=pre_kind,  # phase/status signal, not a breakout confirmation
                    source="orderflow",
                    score=round(pre_score, 2),
                    entry=entry,
                    stop_loss=stop,
                    take_profit_1=tp1,
                    take_profit_2=tp2,
                    reasons=pre_final_reasons,
                    meta={
                        "support": round(support, 8),
                        "resistance": round(resistance, 8),
                        "range_width_pct": round(range_width_pct, 4),
                        "range_compression_ratio": round(range_compression_ratio, 4),
                        "range_duration_minutes": int(range_duration_minutes),
                        "range_duration_bars": int(range_duration_bars),
                        "duration_threshold_minutes": int(duration_threshold_minutes),
                        "body_compression_ratio": round(body_compression_ratio, 4),
                        "wick_to_body_ratio": round(wick_to_body_ratio, 4),
                        "atr_compression": round(atr_compression, 4),
                        "turnover_displacement_ratio": round(turnover_displacement_ratio, 4),
                        "high_displacement_threshold": round(high_displacement_threshold, 4),
                        "wide_range_threshold": round(wide_range_threshold, 4),
                        "delta_notional": round(delta_notional, 2),
                        "buy_notional": round(buy_notional, 2),
                        "sell_notional": round(sell_notional, 2),
                        "compression_ratio": round(compression_ratio, 3),
                        "signal_mode": self.settings.signal_mode,
                        "phase": "pre_impulse_absorption",
                        **tp_meta,
                    },
                )
            )

        early_score = 0.0
        reasons: list[str] = []
        if support_dist <= self.settings.support_tolerance_bps:
            early_score += 1.3
            reasons.append(f"near_support={support_dist:.1f}bps")
        if compression_ratio <= 0.72:
            early_score += 1.1
            reasons.append(f"compression={compression_ratio:.2f}")
        if sell_notional >= self.settings.effective_min_sell_pressure_notional and delta_notional > -sell_notional * 0.55:
            early_score += 1.2
            reasons.append("sell_pressure_absorbed")
        if bid_persistence >= self.settings.effective_min_wall_persistence:
            early_score += min(bid_persistence * 2.8, 1.2)
            reasons.append(f"bid_refill={bid_persistence:.2f}")
        if support_holds >= 4:
            early_score += 0.8
            reasons.append(f"support_holds={support_holds}")
        if top_imbalance >= self.settings.effective_min_book_imbalance:
            early_score += 0.8
            reasons.append(f"bid_imbalance={top_imbalance:.2f}")
        if turnover_build > self.settings.effective_min_trade_notional * 4 and displacement_pct <= 1.2:
            early_score += 0.7
            reasons.append("high_turnover_low_displacement")
        if float(last["close_pos"]) >= 0.58:
            early_score += 0.5
            reasons.append("close_in_upper_half")

        if early_score >= self.settings.effective_min_absorption_score:
            entry = best_ask
            stop = min(support - atr * 0.25, entry - atr * self.settings.atr_stop_mult)
            risk = max(entry - stop, atr * 0.45)
            tp1, tp2, tp_reasons, tp_meta = _find_structural_resistances(
                df=df,
                state=state,
                entry=entry,
                fallback_risk=risk,
                base_resistance=resistance,
                settings=self.settings,
            )
            reward_to_r1 = (tp1 - entry) / max(entry - stop, 1e-12)
            if reward_to_r1 >= self.settings.effective_min_reward_to_r1:
                reasons_early = reasons.copy()
                reasons_early.extend(tp_reasons)
                signals.append(
                    Signal(
                        symbol=symbol,
                        side="Buy",
                        kind="ACCUMULATION_LONG_EARLY" if self.settings.is_early_mode else "ACCUMULATION_LONG_READY",
                        source="orderflow",
                        score=round(early_score, 2),
                        entry=entry,
                        stop_loss=stop,
                        take_profit_1=tp1,
                        take_profit_2=tp2,
                        reasons=reasons_early,
                        meta={
                            "support": round(support, 8),
                            "resistance": round(resistance, 8),
                            "delta_notional": round(delta_notional, 2),
                            "buy_notional": round(buy_notional, 2),
                            "sell_notional": round(sell_notional, 2),
                            "spread_bps": round(spread_bps, 2),
                            "compression_ratio": round(compression_ratio, 3),
                            "reward_to_r1": round(reward_to_r1, 3),
                            "signal_mode": self.settings.signal_mode,
                            **tp_meta,
                        },
                    )
                )

        ready_score = early_score
        ready_reasons = reasons.copy()
        if resistance_dist <= self.settings.effective_breakout_ready_bps:
            ready_score += 1.2
            ready_reasons.append(f"near_breakout={resistance_dist:.1f}bps")
        if top_imbalance >= self.settings.effective_min_book_imbalance * 1.2:
            ready_score += 0.7
            ready_reasons.append("asks_thinning")
        if ask_near_persistence <= 0.14:
            ready_score += 0.7
            ready_reasons.append("no_heavy_ask_wall")
        if buy_notional >= sell_notional * 1.08:
            ready_score += 0.8
            ready_reasons.append("buyers_regaining_tape")
        if pullback_depth_pct <= 0.95:
            ready_score += 0.6
            ready_reasons.append("shallow_pullbacks")
        if float(last["ema_20"]) >= float(last["ema_50"]):
            ready_score += 0.4
            ready_reasons.append("micro_trend_up")

        if ready_score >= self.settings.effective_min_ready_score:
            entry = best_ask
            stop = min(support - atr * 0.2, entry - atr * self.settings.atr_stop_mult)
            risk = max(entry - stop, atr * 0.45)
            tp1, tp2, tp_reasons, tp_meta = _find_structural_resistances(
                df=df,
                state=state,
                entry=entry,
                fallback_risk=risk,
                base_resistance=resistance,
                settings=self.settings,
            )
            reward_to_r1 = (tp1 - entry) / max(entry - stop, 1e-12)
            if reward_to_r1 >= self.settings.effective_min_reward_to_r1:
                ready_reasons_final = ready_reasons.copy()
                ready_reasons_final.extend(tp_reasons)
                signals.append(
                    Signal(
                        symbol=symbol,
                        side="Buy",
                        kind="ACCUMULATION_LONG_READY",
                        source="orderflow",
                        score=round(ready_score, 2),
                        entry=entry,
                        stop_loss=stop,
                        take_profit_1=tp1,
                        take_profit_2=tp2,
                        reasons=ready_reasons_final,
                        meta={
                            "support": round(support, 8),
                            "resistance": round(resistance, 8),
                            "delta_notional": round(delta_notional, 2),
                            "buy_notional": round(buy_notional, 2),
                            "sell_notional": round(sell_notional, 2),
                            "spread_bps": round(spread_bps, 2),
                            "compression_ratio": round(compression_ratio, 3),
                            "resistance_dist_bps": round(resistance_dist, 2),
                            "reward_to_r1": round(reward_to_r1, 3),
                            "signal_mode": self.settings.signal_mode,
                            **tp_meta,
                        },
                    )
                )

        return signals

    def diagnose(self, symbol: str, df: pd.DataFrame, state: SymbolFlowState | None) -> tuple[str, float | None, dict[str, object]]:
        if df.empty or state is None:
            return "missing_df_or_state", None, {}
        if time.time() - state.last_update_ts > self.settings.stale_book_seconds:
            return "stale_book", None, {"age_sec": round(time.time() - state.last_update_ts, 2)}
        df = add_indicators(df)
        if len(df) < 80:
            return "not_enough_bars", None, {"bars": len(df)}
        last = df.iloc[-1]
        close = float(last["close"])
        support = local_support(df.iloc[:-1], self.settings.support_lookback_bars)
        resistance = local_resistance(df.iloc[:-1], self.settings.resistance_lookback_bars)
        if support is None or resistance is None or resistance <= support:
            return "bad_structure_levels", None, {}
        best_bid, best_ask = _best_bid_ask(state)
        if best_bid is None or best_ask is None:
            return "book_empty", None, {}
        mid = state.last_mid or close
        spread_bps = (best_ask - best_bid) / max(mid, 1e-12) * 10000.0
        if spread_bps > self.settings.max_spread_bps:
            return "spread_too_wide", None, {"spread_bps": round(spread_bps, 2)}
        top_imbalance = _top_imbalance(state, self.settings.book_depth)
        buy_notional = _sum_trade_notional(state, "Buy")
        sell_notional = _sum_trade_notional(state, "Sell")
        tape_total = buy_notional + sell_notional
        delta_notional = _signed_delta_notional(state)
        if tape_total < self.settings.effective_min_trade_notional:
            return "trade_notional_too_low", None, {"tape_total": round(tape_total,2), "min": self.settings.effective_min_trade_notional}
        support_dist = _bps_distance(mid, support)
        bid_persistence = _wall_persistence_ratio(state.bid_walls, support, self.settings.support_tolerance_bps)
        support_holds = _support_hold_count(df, support, self.settings.support_tolerance_bps, bars=15)
        base_range_short = rolling_range_pct(df, 14)
        base_range_long = rolling_range_pct(df, 48)
        compression_ratio = base_range_short / max(base_range_long, 1e-9)
        turnover_build = float(pd.to_numeric(df.tail(12)["turnover"], errors="coerce").sum())
        displacement_pct = abs(float(pd.to_numeric(df.tail(12)["return_1"], errors="coerce").sum() * 100.0))
        early_score = 0.0
        if support_dist <= self.settings.support_tolerance_bps:
            early_score += 1.3
        if compression_ratio <= 0.72:
            early_score += 1.1
        if sell_notional >= self.settings.effective_min_sell_pressure_notional and delta_notional > -sell_notional * 0.55:
            early_score += 1.2
        if bid_persistence >= self.settings.effective_min_wall_persistence:
            early_score += min(bid_persistence * 2.8, 1.2)
        if support_holds >= 4:
            early_score += 0.8
        if top_imbalance >= self.settings.effective_min_book_imbalance:
            early_score += 0.8
        if turnover_build > self.settings.effective_min_trade_notional * 4 and displacement_pct <= 1.2:
            early_score += 0.7
        if float(last["close_pos"]) >= 0.58:
            early_score += 0.5
        metrics = {"early_score": round(early_score,3), "support_dist_bps": round(support_dist,2), "imbalance": round(top_imbalance,3), "bid_persistence": round(bid_persistence,3), "support_holds": support_holds, "compression_ratio": round(compression_ratio,3)}
        if early_score < self.settings.effective_min_absorption_score:
            return "early_score_too_low", early_score, metrics
        atr = max(float(last["atr_14"]), close * 0.0028)
        entry = best_ask
        stop = min(support - atr * 0.25, entry - atr * self.settings.atr_stop_mult)
        risk = max(entry - stop, atr * 0.45)
        tp1, tp2, _, tp_meta = _find_structural_resistances(df=df, state=state, entry=entry, fallback_risk=risk, base_resistance=resistance, settings=self.settings)
        reward_to_r1 = (tp1 - entry) / max(entry - stop, 1e-12)
        metrics.update({"reward_to_r1": round(reward_to_r1,3), **tp_meta})
        if reward_to_r1 < self.settings.effective_min_reward_to_r1:
            return "reward_to_r1_too_low", early_score, metrics
        return "candidate_should_signal", early_score, metrics
