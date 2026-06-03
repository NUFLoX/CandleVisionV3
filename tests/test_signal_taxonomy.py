from __future__ import annotations

from orderflow_accum.signal_taxonomy import (
    EXECUTION_STABLE_KINDS,
    EXPERIMENTAL_KINDS,
    HIGH_POTENTIAL_KINDS,
    is_high_potential_kind,
    normalize_signal_kind,
    signal_family,
    signal_focus_group,
)


def test_signal_taxonomy_maps_all_key_kinds() -> None:
    expected = {
        "ACCUMULATION_WATCH": ("HIGH_POTENTIAL_ACCUMULATION", "HIGH_POTENTIAL"),
        "ABSORPTION_ZONE": ("HIGH_POTENTIAL_ABSORPTION", "HIGH_POTENTIAL"),
        "PRE_IMPULSE_ZONE": ("HIGH_POTENTIAL_PRE_IMPULSE", "HIGH_POTENTIAL"),
        "BREAKOUT_PRESSURE": ("EXECUTION_STABLE_BREAKOUT", "EXECUTION_STABLE"),
        "ACCUMULATION_LONG_EARLY": ("EXPERIMENTAL_EARLY", "EXPERIMENTAL"),
        "ACCUMULATION_LONG_READY": ("EXPERIMENTAL_READY", "EXPERIMENTAL"),
        "BASE_BUILDUP_LONG": ("EXPERIMENTAL_BASE_BUILDUP", "EXPERIMENTAL"),
    }

    for kind, (family, focus_group) in expected.items():
        assert signal_family(kind) == family
        assert signal_focus_group(kind) == focus_group


def test_signal_taxonomy_normalizes_case_and_handles_unknown() -> None:
    assert normalize_signal_kind(" accumulation_watch ") == "ACCUMULATION_WATCH"
    assert signal_family(" accumulation_watch ") == "HIGH_POTENTIAL_ACCUMULATION"
    assert signal_focus_group("missing_kind") == "OTHER"
    assert signal_family(None) == "OTHER"
    assert not is_high_potential_kind("BREAKOUT_PRESSURE")


def test_signal_taxonomy_kind_sets_are_dashboard_only_constants() -> None:
    assert HIGH_POTENTIAL_KINDS == {"ACCUMULATION_WATCH", "ABSORPTION_ZONE", "PRE_IMPULSE_ZONE"}
    assert EXECUTION_STABLE_KINDS == {"BREAKOUT_PRESSURE"}
    assert EXPERIMENTAL_KINDS == {"ACCUMULATION_LONG_EARLY", "ACCUMULATION_LONG_READY", "BASE_BUILDUP_LONG"}
    assert is_high_potential_kind("PRE_IMPULSE_ZONE")
