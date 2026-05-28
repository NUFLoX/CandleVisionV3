from __future__ import annotations

from dataclasses import dataclass


GOOD_LONG_REASONS = {
    "sell_pressure_absorbed",
    "support_defended",
    "close_in_upper_half",
    "high_turnover_low_displacement",
    "candle_body_compression",
    "no_heavy_ask_wall",
}

GOOD_SHORT_REASONS = {
    "buy_pressure_absorbed",
    "resistance_defended",
    "close_in_lower_half",
    "high_turnover_low_displacement",
    "bid_wall_weakening",
}

BAD_REASONS = {"rr_fallback", "strong_ask_wall", "bad_spread"}

WATCHLIST_LONG_STATUSES = {"WATCHING", "ACCUMULATION", "PRE_IMPULSE"}
WATCHLIST_SHORT_STATUSES = {"SHORT_WATCH", "DISTRIBUTION_ZONE", "PRE_DUMP_ZONE"}


@dataclass(slots=True)
class PromotionDecision:
    should_promote: bool
    target_status: str | None
    reasons: list[str]


class ConfirmedPromoter:
    def __init__(self, min_score: float = 8.0, min_repeat_count: int = 2):
        self.min_score = min_score
        self.min_repeat_count = min_repeat_count

    def should_promote(self, setup: dict, current_features: dict | None, regime: dict | None) -> PromotionDecision:
        features = current_features or {}
        btc_regime = str((regime or {}).get("btc_regime") or setup.get("btc_regime") or "BTC_NEUTRAL").upper()
        side = str(setup.get("side") or "Buy")
        market = str(setup.get("market") or "linear").lower()
        status = str(setup.get("status") or "PENDING").upper()
        score_first = float(setup.get("score_first") or 0.0)
        score_last = float(setup.get("score_last") or 0.0)
        repeat_count = int(setup.get("repeat_count") or 0)
        timeframe = str(setup.get("timeframe") or "1")
        reasons = {str(x) for x in (features.get("reasons") or setup.get("reasons") or [])}

        why: list[str] = []
        if score_last < self.min_score:
            why.append("score_below_min")
        if score_last <= score_first:
            why.append("score_not_increasing")
        if repeat_count < self.min_repeat_count:
            why.append("repeat_count_too_low")
        if timeframe == "1":
            why.append("timeframe_1m_blocked")
        if reasons & BAD_REASONS:
            why.append("bad_reason_present")

        if side.lower() == "buy":
            if status not in WATCHLIST_LONG_STATUSES:
                why.append("not_watchlist_long_status")
            if btc_regime in {"BTC_BEARISH", "BTC_DUMP_RISK"}:
                why.append("btc_regime_blocks_long")
            if len(reasons & GOOD_LONG_REASONS) < 3:
                why.append("insufficient_good_long_reasons")
            if why:
                return PromotionDecision(False, None, why)
            return PromotionDecision(True, "CONFIRMED_LONG", ["long_promotion_rules_met"])

        if side.lower() == "sell":
            if market != "linear":
                why.append("short_non_linear_blocked")
            if status not in WATCHLIST_SHORT_STATUSES:
                why.append("not_watchlist_short_status")
            if btc_regime not in {"BTC_BEARISH", "BTC_DUMP_RISK"}:
                why.append("btc_regime_not_bearish_for_short")
            if len(reasons & GOOD_SHORT_REASONS) < 3:
                why.append("insufficient_good_short_reasons")
            if why:
                return PromotionDecision(False, None, why)
            return PromotionDecision(True, "CONFIRMED_SHORT", ["short_promotion_rules_met"])

        return PromotionDecision(False, None, ["unsupported_side"])
