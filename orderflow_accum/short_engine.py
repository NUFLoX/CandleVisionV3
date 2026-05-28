from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from .bookflow import SymbolFlowState
from .engines import _bps_distance
from .models import Signal


@dataclass(slots=True)
class ShortSettings:
    min_score: float = 6.0


class DistributionShortEngine:
    def __init__(self, settings):
        self.settings = settings

    def analyze(self, symbol: str, df: pd.DataFrame, state: SymbolFlowState | None, regime) -> list[Signal]:
        if df.empty or state is None or len(df) < 40:
            return []
        if regime is None:
            return []
        if self.settings.short_only_when_btc_bearish and not regime.shorts_enabled:
            return []

        last = df.iloc[-1]
        close = float(last["close"])
        support = float(pd.to_numeric(df.tail(30)["low"], errors="coerce").min())
        resistance = float(pd.to_numeric(df.tail(30)["high"], errors="coerce").max())
        range_width_pct = (resistance - support) / max(close, 1e-12) * 100.0
        close_pos = (close - support) / max(resistance - support, 1e-12)

        sells = sum(t.notional for t in state.trades if t.side == "Sell")
        buys = sum(t.notional for t in state.trades if t.side == "Buy")
        tape_total = sells + buys

        score = 0.0
        reasons: list[str] = []
        if range_width_pct <= 3.4:
            score += 2.0; reasons.append("long_sideways_range")
        body_last_20 = float(pd.to_numeric((df.tail(20)["close"] - df.tail(20)["open"]).abs(), errors="coerce").mean())
        body_prev_20 = float(pd.to_numeric((df.tail(40).head(20)["close"] - df.tail(40).head(20)["open"]).abs(), errors="coerce").mean())
        if body_last_20 <= body_prev_20 * 0.75:
            score += 2.0; reasons.append("candle_body_compression")
        if tape_total > self.settings.effective_min_trade_notional * 1.1 and sells >= buys * 0.95:
            score += 2.0; reasons.append("buy_pressure_absorbed")
        if close_pos <= 0.44:
            score += 1.0; reasons.append("close_in_lower_half")
        low_dist = _bps_distance(close, support)
        if low_dist <= self.settings.effective_breakout_ready_bps * 1.2:
            score += 2.0; reasons.append("price_near_range_low")
        if sells >= buys * 1.05:
            score += 1.0; reasons.append("sellers_regaining_tape")
        if float(last.get("ema_20", close)) < float(last.get("ema_50", close)):
            score += 1.0; reasons.append("micro_trend_down")

        score += float(regime.short_bonus or 0.0)

        if score < self.settings.short_min_score:
            return []

        entry = close
        sl = max(resistance * 1.003, entry + (resistance - support) * 0.25)
        tp1 = min(support * 1.002, entry - max((sl - entry) * 1.0, entry * 0.01))
        tp2 = min(tp1 * 0.985, entry - max((sl - entry) * 1.8, entry * 0.02))

        kind = "PRE_DUMP_ZONE" if score < self.settings.short_min_score + 2 else "BREAKDOWN_PRESSURE"
        return [Signal(symbol=symbol, side="Sell", kind=kind, source="orderflow", score=round(score, 2), entry=entry, stop_loss=sl, take_profit_1=tp1, take_profit_2=tp2, reasons=reasons, meta={"phase": "distribution_short", "btc_regime": regime.btc_regime})]
