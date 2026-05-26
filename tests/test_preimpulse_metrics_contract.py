from __future__ import annotations

from pathlib import Path


def test_preimpulse_contract_contains_v2_metrics_and_reasons() -> None:
    src = (Path(__file__).resolve().parents[1] / "orderflow_accum" / "engines.py").read_text(encoding="utf-8")

    required_tokens = [
        "sell_pressure_absorbed_v2",
        "range_duration_minutes",
        "wick_to_body_ratio",
        "range_compression_ratio",
        "turnover_displacement_ratio",
    ]
    for token in required_tokens:
        assert token in src
