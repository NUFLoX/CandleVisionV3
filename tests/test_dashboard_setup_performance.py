from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

import dashboard.server as server_module


def test_setup_performance_endpoint_declared_and_returns_groups() -> None:
    src_path = Path(__file__).resolve().parents[1] / "dashboard" / "server.py"
    source = src_path.read_text(encoding="utf-8")
    ast.parse(source)

    has_route = '"/api/setup-performance"' in source
    assert has_route

    # Ensure response keys are present in function source/body literals
    assert '"by_reason"' in source
    assert '"by_score_bucket"' in source
    assert '"by_timeframe"' in source
    assert '"by_kind"' in source
    assert '"by_source"' in source
    assert '"by_family"' in source
    assert '"by_focus_group"' in source


def _init_setup_performance_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE signals (
            reasons_last TEXT,
            score_last REAL,
            timeframe TEXT,
            kind TEXT,
            source TEXT,
            status TEXT,
            max_gain_pct REAL,
            max_drawdown_pct REAL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO signals (
            reasons_last,score_last,timeframe,kind,source,status,max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        [
            ('["volume_pressure"]', 8.0, "15", "ACCUMULATION_WATCH", "orderflow", "TP2", 5.0, -1.0),
            ('["absorption"]', 7.0, "15", "ABSORPTION_ZONE", "orderflow", "SL", 2.0, -3.0),
            ('["breakout"]', 10.0, "5", "BREAKOUT_PRESSURE", "scanner", "OTHER", 6.0, -0.5),
            ('["early"]', 4.0, "60", "ACCUMULATION_LONG_EARLY", "scanner", "TP1", 3.0, -1.5),
            ('["unknown"]', 3.0, "60", "UNEXPECTED_KIND", "scanner", "WATCHING", 1.0, -2.0),
        ],
    )
    conn.commit()
    conn.close()


def test_setup_performance_groups_use_signal_taxonomy_mappings(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_setup_performance_db(db_path)
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/setup-performance")

    assert response.status_code == 200
    payload = response.json()

    focus_rows = {row["focus_group"]: row for row in payload["by_focus_group"]}
    assert list(focus_rows) == ["HIGH_POTENTIAL", "EXECUTION_STABLE", "EXPERIMENTAL", "OTHER"]
    assert focus_rows["HIGH_POTENTIAL"]["total"] == 2
    assert focus_rows["EXECUTION_STABLE"]["total"] == 1
    assert focus_rows["EXPERIMENTAL"]["total"] == 1
    assert focus_rows["OTHER"]["total"] == 1

    family_rows = {row["family"]: row for row in payload["by_family"]}
    assert list(family_rows) == [
        "HIGH_POTENTIAL_ACCUMULATION",
        "HIGH_POTENTIAL_ABSORPTION",
        "HIGH_POTENTIAL_PRE_IMPULSE",
        "EXECUTION_STABLE_BREAKOUT",
        "EXPERIMENTAL_EARLY",
        "EXPERIMENTAL_READY",
        "EXPERIMENTAL_BASE_BUILDUP",
        "OTHER",
    ]
    assert family_rows["HIGH_POTENTIAL_ACCUMULATION"]["total"] == 1
    assert family_rows["HIGH_POTENTIAL_ABSORPTION"]["total"] == 1
    assert family_rows["HIGH_POTENTIAL_PRE_IMPULSE"]["total"] == 0
    assert family_rows["EXECUTION_STABLE_BREAKOUT"]["total"] == 1
    assert family_rows["EXPERIMENTAL_EARLY"]["total"] == 1
    assert family_rows["EXPERIMENTAL_READY"]["total"] == 0
    assert family_rows["EXPERIMENTAL_BASE_BUILDUP"]["total"] == 0
    assert family_rows["OTHER"]["total"] == 1


def test_dashboard_frontend_still_renders_signal_intelligence() -> None:
    frontend = (Path(__file__).resolve().parents[1] / "dashboard" / "static" / "index.html").read_text(encoding="utf-8")

    assert "Signal Intelligence" in frontend
    assert "Legacy Signal Performance" in frontend
    assert "Legacy signal_stats.db outcome model — not SmartTradeExecutor paper ledger." in frontend
