from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.outcome_tracker import evaluate_outcome_for_side


def _c(h,l):
    return {"high":h,"low":l}


def test_short_tp_hit_when_low_reaches_tp():
    out = evaluate_outcome_for_side(100, 105, 98, 96, [_c(101,97.9)], 1, side="Sell")
    assert out.status == "TP1"


def test_short_sl_hit_when_high_reaches_sl():
    out = evaluate_outcome_for_side(100, 105, 98, 96, [_c(105.1,99)], 1, side="Sell")
    assert out.status == "SL"


def test_short_ambiguous_when_tp_and_sl_same_candle():
    out = evaluate_outcome_for_side(100, 105, 98, 96, [_c(105.1,95.9)], 1, side="Sell")
    assert out.status == "AMBIGUOUS"
