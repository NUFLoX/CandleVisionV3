from __future__ import annotations

import ast
from pathlib import Path


def test_runner_realtime_loop_iterates_configured_intervals() -> None:
    src_path = Path(__file__).resolve().parents[1] / "orderflow_accum" / "runner.py"
    tree = ast.parse(src_path.read_text(encoding="utf-8"))

    found = False
    for node in ast.walk(tree):
        if isinstance(node, (ast.For, ast.AsyncFor)):
            # target: interval ; iter: self.settings.realtime_intervals
            if isinstance(node.target, ast.Name) and node.target.id == "interval":
                it = node.iter
                if (
                    isinstance(it, ast.Attribute)
                    and it.attr == "realtime_intervals"
                    and isinstance(it.value, ast.Attribute)
                    and it.value.attr == "settings"
                ):
                    found = True
                    break

    assert found, "Expected async loop over self.settings.realtime_intervals in _run_realtime_scan"
