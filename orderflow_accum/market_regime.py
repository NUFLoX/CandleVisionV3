from __future__ import annotations

from dataclasses import dataclass, field
import pandas as pd


@dataclass(slots=True)
class MarketRegime:
    btc_regime: str = "BTC_NEUTRAL"
    shorts_enabled: bool = False
    long_penalty: float = 0.0
    short_bonus: float = 0.0
    reasons: list[str] = field(default_factory=list)
    market_regime: str | None = None

    def __post_init__(self) -> None:
        if self.market_regime is None:
            self.market_regime = self.btc_regime


class MarketRegimeAnalyzer:
    def __init__(self, *, short_bonus: float = 2.0, long_bearish_penalty: float = -2.0) -> None:
        self.short_bonus = short_bonus
        self.long_bearish_penalty = long_bearish_penalty

    def analyze_btc(self, frames: dict[str, pd.DataFrame]) -> MarketRegime:
        if not frames:
            return MarketRegime()
        # prefer 60m if available




        df = frames.get("60")
        if df is None:
            df = next(iter(frames.values()))

        if df is None or df.empty or len(df) < 4:

            return MarketRegime()

        last = df.iloc[-1]
        close = float(last.get("close", 0.0) or 0.0)
        ema20 = float(last.get("ema_20", close) or close)
        ema50 = float(last.get("ema_50", close) or close)
        reasons: list[str] = []

        bearish = 0
        bullish = 0
        if close < ema20:
            bearish += 1
            reasons.append("btc_below_ema20")
        elif close > ema20:
            bullish += 1
        if close < ema50:
            bearish += 1
            reasons.append("btc_below_ema50")
        elif close > ema50:
            bullish += 1

        if len(df) >= 4:
            ema20_prev = float(df.iloc[-4].get("ema_20", ema20) or ema20)
            if ema20 < ema20_prev:
                bearish += 1
                reasons.append("ema20_down")
            elif ema20 > ema20_prev:
                bullish += 1

        close_pos = float(last.get("close_pos", 0.5) or 0.5)
        if close_pos < 0.45:
            bearish += 1
            reasons.append("close_lower_half")
        elif close_pos > 0.60:
            bullish += 1

        if bearish >= 4:
            return MarketRegime("BTC_DUMP_RISK", True, self.long_bearish_penalty, self.short_bonus + 0.5, reasons)
        if bearish >= 2 and bearish > bullish:
            return MarketRegime("BTC_BEARISH", True, self.long_bearish_penalty, self.short_bonus, reasons)
        if bullish >= 3:
            return MarketRegime("BTC_BULLISH", False, 0.5, -2.0, ["btc_bullish_structure"])
        return MarketRegime("BTC_NEUTRAL", False, 0.0, 0.0, reasons)
