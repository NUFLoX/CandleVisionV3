from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orderflow_accum.confirmed_promoter import ConfirmedPromoter


def test_confirmed_promoter_promotes_good_watchlist() -> None:
    p = ConfirmedPromoter(min_score=8.0, min_repeat_count=2)
    setup = {
        "side": "Buy", "market": "linear", "status": "PRE_IMPULSE", "score_first": 6.0,
        "score_last": 9.0, "repeat_count": 2, "timeframe": "15",
        "reasons": ["sell_pressure_absorbed", "support_defended", "close_in_upper_half"],
    }
    d = p.should_promote(setup, {}, {"btc_regime": "BTC_BULLISH"})
    assert d.should_promote is True
    assert d.target_status == "CONFIRMED_LONG"


def test_confirmed_promoter_blocks_bad_btc_for_long() -> None:
    p = ConfirmedPromoter()
    setup = {
        "side": "Buy", "market": "linear", "status": "PRE_IMPULSE", "score_first": 6.0,
        "score_last": 9.0, "repeat_count": 3, "timeframe": "15",
        "reasons": ["sell_pressure_absorbed", "support_defended", "close_in_upper_half"],
    }
    d = p.should_promote(setup, {}, {"btc_regime": "BTC_BEARISH"})
    assert d.should_promote is False
    assert "btc_regime_blocks_long" in d.reasons


def test_confirmed_promoter_allows_short_when_btc_bearish() -> None:
    p = ConfirmedPromoter()
    setup = {
        "side": "Sell", "market": "linear", "status": "PRE_DUMP_ZONE", "score_first": 7.0,
        "score_last": 9.0, "repeat_count": 2, "timeframe": "15",
        "reasons": ["buy_pressure_absorbed", "resistance_defended", "close_in_lower_half"],
    }
    d = p.should_promote(setup, {}, {"btc_regime": "BTC_BEARISH"})
    assert d.should_promote is True
    assert d.target_status == "CONFIRMED_SHORT"