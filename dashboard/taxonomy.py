from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SignalTaxonomy:
    """Dashboard-only labels derived from existing signal metadata.

    These labels are intentionally descriptive. They must not be used by scanners,
    signal stores, promotion, or execution logic to decide whether a signal exists,
    changes status, changes score, or enters/exits a trade.
    """

    signal_kind: str
    signal_family: str
    signal_focus_group: str
    signal_source: str
    signal_timeframe: str


def build_signal_taxonomy(
    *,
    kind: object,
    source: object,
    timeframe: object,
) -> SignalTaxonomy:
    signal_kind = _clean_label(kind, "SIGNAL")
    signal_source = _clean_label(source, "scanner")
    signal_timeframe = _normalize_timeframe(timeframe)
    return SignalTaxonomy(
        signal_kind=signal_kind,
        signal_family=_family_from_kind(signal_kind),
        signal_focus_group=_focus_group_from_kind(signal_kind),
        signal_source=signal_source,
        signal_timeframe=signal_timeframe,
    )


def taxonomy_from_signal(signal: Any, *, timeframe: object = "live") -> SignalTaxonomy:
    kind = getattr(signal, "kind", None)
    source = getattr(signal, "source", None)
    meta = dict(getattr(signal, "meta", {}) or {})
    raw_timeframe = meta.get("tf") or timeframe
    return build_signal_taxonomy(kind=kind, source=source, timeframe=raw_timeframe)


def _clean_label(value: object, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def _family_from_kind(kind: str) -> str:
    text = kind.upper()
    if any(token in text for token in ("SHORT", "DISTRIBUTION", "DUMP", "BREAKDOWN")):
        return "short_distribution"
    if any(token in text for token in ("ACCUMULATION", "ABSORPTION", "IMPULSE", "BREAKOUT", "BASE")):
        return "long_accumulation"
    return "unclassified"


def _focus_group_from_kind(kind: str) -> str:
    text = kind.upper()
    if "WATCH" in text or "BASE" in text:
        return "watchlist"
    if "ABSORPTION" in text or "DISTRIBUTION" in text:
        return "positioning"
    if "PRE_IMPULSE" in text or "PRE_DUMP" in text:
        return "pre_move"
    if "BREAKOUT" in text or "BREAKDOWN" in text or "CONFIRMED" in text or "READY" in text:
        return "confirmed_pressure"
    return "unclassified"


def _normalize_timeframe(tf: object) -> str:
    value = str(tf or "").strip().upper()
    mapping = {
        "1": "1m",
        "3": "3m",
        "5": "5m",
        "15": "15m",
        "30": "30m",
        "60": "1h",
        "120": "2h",
        "240": "4h",
        "D": "1d",
        "W": "1w",
        "LIVE": "live",
    }
    if not value:
        return "1m"
    return mapping.get(value, value.lower())
