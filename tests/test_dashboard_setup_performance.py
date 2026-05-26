from __future__ import annotations

import ast
from pathlib import Path


def test_setup_performance_endpoint_declared_and_returns_groups() -> None:
    src_path = Path(__file__).resolve().parents[1] / "dashboard" / "server.py"
    source = src_path.read_text(encoding="utf-8")
    tree = ast.parse(source)

    has_route = '"/api/setup-performance"' in source
    assert has_route

    # Ensure response keys are present in function source/body literals
    assert '"by_reason"' in source
    assert '"by_score_bucket"' in source
    assert '"by_timeframe"' in source
