from __future__ import annotations

HIGH_POTENTIAL_KINDS = {"ACCUMULATION_WATCH", "ABSORPTION_ZONE", "PRE_IMPULSE_ZONE"}
EXECUTION_STABLE_KINDS = {"BREAKOUT_PRESSURE"}
EXPERIMENTAL_KINDS = {"ACCUMULATION_LONG_EARLY", "ACCUMULATION_LONG_READY", "BASE_BUILDUP_LONG"}

_SIGNAL_FAMILIES = {
    "ACCUMULATION_WATCH": "HIGH_POTENTIAL_ACCUMULATION",
    "ABSORPTION_ZONE": "HIGH_POTENTIAL_ABSORPTION",
    "PRE_IMPULSE_ZONE": "HIGH_POTENTIAL_PRE_IMPULSE",
    "BREAKOUT_PRESSURE": "EXECUTION_STABLE_BREAKOUT",
    "ACCUMULATION_LONG_EARLY": "EXPERIMENTAL_EARLY",
    "ACCUMULATION_LONG_READY": "EXPERIMENTAL_READY",
    "BASE_BUILDUP_LONG": "EXPERIMENTAL_BASE_BUILDUP",
}

_SIGNAL_FOCUS_GROUPS = {
    **{kind: "HIGH_POTENTIAL" for kind in HIGH_POTENTIAL_KINDS},
    **{kind: "EXECUTION_STABLE" for kind in EXECUTION_STABLE_KINDS},
    **{kind: "EXPERIMENTAL" for kind in EXPERIMENTAL_KINDS},
}


def normalize_signal_kind(kind: object) -> str:
    """Normalize an existing signal kind for dashboard-only taxonomy lookups."""

    return str(kind or "").strip().upper()


def signal_family(kind: object) -> str:
    """Return the dashboard-only family label for a signal kind."""

    return _SIGNAL_FAMILIES.get(normalize_signal_kind(kind), "OTHER")


def signal_focus_group(kind: object) -> str:
    """Return the dashboard-only focus group for a signal kind."""

    return _SIGNAL_FOCUS_GROUPS.get(normalize_signal_kind(kind), "OTHER")


def is_high_potential_kind(kind: object) -> bool:
    """True when a signal kind belongs to the high-potential dashboard group."""

    return normalize_signal_kind(kind) in HIGH_POTENTIAL_KINDS
