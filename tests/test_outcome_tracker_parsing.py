from __future__ import annotations

from pathlib import Path
import sys
from datetime import timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.outcome_tracker import _parse_time, _interval_to_minutes


def test_parse_time_accepts_z_suffix_as_utc() -> None:
    dt = _parse_time("2026-05-26T12:34:56Z")
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.utcoffset() == timezone.utc.utcoffset(dt)


def test_parse_time_invalid_returns_none() -> None:
    assert _parse_time("not-a-timestamp") is None


def test_interval_to_minutes_mapping() -> None:
    assert _interval_to_minutes("1") == 1
    assert _interval_to_minutes("15") == 15
    assert _interval_to_minutes("60") == 60
    assert _interval_to_minutes("240") == 240
    assert _interval_to_minutes("D") == 1440
    assert _interval_to_minutes("unknown") == 1
