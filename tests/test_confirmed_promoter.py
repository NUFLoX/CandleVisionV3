from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orderflow_accum.confirmed_promoter import ConfirmedPromoter


def _base_buy_setup() -> dict:
    return {
        "side": "Buy",
        "market": "linear",
        "status": "PRE_IMPULSE",
        "score_first": 6.0,
        "score_last": 9.0,
        "repeat_count": 3,
        "timeframe": "15m",
        "reasons": [
            "sell_pressure_absorbed",
            "support_defended",
            "close_in_upper_half",
        ],
        "time_to_0_5R_minutes": 25,
    }


def _base_sell_setup() -> dict:
    return {
        "side": "Sell",
        "market": "linear",
        "status": "PRE_DUMP",
        "score_first": 6.5,
        "score_last": 8.8,
        "repeat_count": 2,
        "timeframe": "15m",
        "reasons": [
            "buy_pressure_absorbed",
            "resistance_defended",
            "close_in_lower_half",
        ],
        "time_to_0_5R_minutes": 30,
    }


def test_good_buy_promotes_to_confirmed_long() -> None:
    p = ConfirmedPromoter()
    d = p.should_promote(_base_buy_setup(), {}, {"btc_regime": "BTC_BULLISH"})
    assert d.should_promote is True
    assert d.target_status == "CONFIRMED_LONG"


def test_buy_blocked_by_btc_bearish() -> None:
    p = ConfirmedPromoter()
    d = p.should_promote(_base_buy_setup(), {}, {"btc_regime": "BTC_BEARISH"})
    assert d.should_promote is False
    assert "btc_regime_blocks_long" in d.reasons


def test_buy_blocked_by_rr_fallback() -> None:
    p = ConfirmedPromoter()
    s = _base_buy_setup()
    s["reasons"] = list(s["reasons"]) + ["rr_fallback"]
    d = p.should_promote(s, {}, {"btc_regime": "BTC_BULLISH"})
    assert d.should_promote is False
    assert "bad_reason_present" in d.reasons


def test_buy_blocked_on_1m_timeframe() -> None:
    p = ConfirmedPromoter()
    s = _base_buy_setup()
    s["timeframe"] = "1m"
    d = p.should_promote(s, {}, {"btc_regime": "BTC_BULLISH"})
    assert d.should_promote is False
    assert "timeframe_1m_blocked" in d.reasons


def test_weak_buy_not_promoted() -> None:
    p = ConfirmedPromoter()
    s = _base_buy_setup()
    s["score_last"] = 7.0
    s["repeat_count"] = 1
    s["reasons"] = ["support_defended"]
    d = p.should_promote(s, {}, {"btc_regime": "BTC_BULLISH"})
    assert d.should_promote is False
    assert "score_below_min" in d.reasons


def test_good_sell_promotes_when_btc_bearish() -> None:
    p = ConfirmedPromoter()
    d = p.should_promote(_base_sell_setup(), {}, {"btc_regime": "BTC_BEARISH"})
    assert d.should_promote is True
    assert d.target_status == "CONFIRMED_SHORT"


def test_sell_blocked_when_market_is_spot() -> None:
    p = ConfirmedPromoter()
    s = _base_sell_setup()
    s["market"] = "spot"
    d = p.should_promote(s, {}, {"btc_regime": "BTC_BEARISH"})
    assert d.should_promote is False
    assert "short_non_linear_blocked" in d.reasons


def test_sell_blocked_when_btc_not_bearish() -> None:
    p = ConfirmedPromoter()
    d = p.should_promote(_base_sell_setup(), {}, {"btc_regime": "BTC_BULLISH"})
    assert d.should_promote is False
    assert "btc_regime_not_bearish_for_short" in d.reasons
