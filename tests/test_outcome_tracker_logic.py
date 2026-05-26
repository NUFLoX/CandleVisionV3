from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.outcome_tracker import evaluate_outcome


def _c(high: float, low: float) -> dict:
    return {"high": high, "low": low}


def test_outcome_tp_before_sl() -> None:
    rows = [_c(112.0, 99.5), _c(108.0, 95.0)]
    out = evaluate_outcome(
        entry=100.0,
        sl=95.0,
        tp1=105.0,
        tp2=110.0,
        rows=rows,
        interval_min=1,
    )
    assert out.status == "TP2"
    assert out.time_to_tp1_minutes == 1
    assert out.time_to_tp2_minutes == 1
    assert out.time_to_sl_minutes is None


def test_outcome_sl_before_tp() -> None:
    rows = [_c(103.0, 94.9), _c(111.0, 100.0)]
    out = evaluate_outcome(
        entry=100.0,
        sl=95.0,
        tp1=105.0,
        tp2=110.0,
        rows=rows,
        interval_min=5,
    )
    assert out.status == "SL"
    assert out.time_to_sl_minutes == 5
    assert out.time_to_tp1_minutes is None


def test_outcome_ambiguous_same_candle() -> None:
    rows = [_c(111.0, 94.0)]
    out = evaluate_outcome(
        entry=100.0,
        sl=95.0,
        tp1=105.0,
        tp2=110.0,
        rows=rows,
        interval_min=1,
    )
    assert out.status == "AMBIGUOUS"
    assert out.time_to_tp1_minutes == 1
    assert out.time_to_tp2_minutes == 1
    assert out.time_to_sl_minutes == 1
