from __future__ import annotations

import py_compile
from pathlib import Path

import pytest


def test_dashboard_server_has_no_merge_artifacts_and_imports() -> None:
    server_path = Path("dashboard/server.py")
    source = server_path.read_text(encoding="utf-8")

    forbidden_tokens = ("<<<<<<<", "=======", ">>>>>>>", "codex/")
    assert not any(token in source for token in forbidden_tokens)

    py_compile.compile(str(server_path), doraise=True)

    pytest.importorskip("fastapi")
    from dashboard.server import app

    assert app.title == "CandleVision Dashboard API"
