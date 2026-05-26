from __future__ import annotations

import ast
from pathlib import Path


def test_scanner_source_has_no_dashboard_imports() -> None:
    src_path = Path(__file__).resolve().parents[1] / "scout" / "scanner.py"
    tree = ast.parse(src_path.read_text(encoding="utf-8"))

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imported.add(n.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imported.add(node.module)

    assert all(not name.startswith("dashboard") for name in imported)
