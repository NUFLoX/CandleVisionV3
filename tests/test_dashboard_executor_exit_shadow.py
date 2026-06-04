from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import dashboard.server as server_module


def _client_for_db(db_path: Path, monkeypatch) -> TestClient:
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    return TestClient(server_module.create_app(), raise_server_exceptions=False)


def test_executor_exit_shadow_missing_db_returns_safe_empty_payload(monkeypatch, tmp_path: Path) -> None:
    with _client_for_db(tmp_path / "missing.db", monkeypatch) as client:
        response = client.get("/api/executor-exit-shadow")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["open_shadow_count"] == 0
    assert payload["summary"]["closed_with_shadow_count"] == 0
    assert payload["open_shadow_trades"] == []
    assert payload["closed_shadow_results"] == []
    assert payload["shadow_events"] == []


def test_executor_exit_shadow_endpoint_returns_open_closed_and_events(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE executor_outcomes (
                signal_key TEXT, symbol TEXT, side TEXT, state TEXT, action TEXT,
                entry_price REAL, current_sl REAL, max_gain_r REAL, updated_at TEXT, diagnostics_json TEXT
            )
        """)
        conn.execute("""
            INSERT INTO executor_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "ETHUSDT|linear|5|CONFIRMED_LONG|Buy", "ETHUSDT", "Buy", "ENTERED", "HOLD",
            100.0, 90.0, 1.2, "2026-06-04T10:00:00+00:00",
            json.dumps({"exit_shadow_enabled": True, "exit_shadow_policy": "trailing_40pct_giveback_after_1r", "exit_shadow_peak_r": 1.2, "exit_shadow_floor_r": 0.72, "exit_shadow_current_r": 0.7, "exit_shadow_triggered": True, "exit_shadow_triggered_at": "2026-06-04T10:00:00+00:00"}),
        ))
        conn.execute("""
            CREATE TABLE executor_trades (
                symbol TEXT, timeframe TEXT, side TEXT, state TEXT, r_result REAL, max_gain_r REAL,
                exit_reason TEXT, exit_time TEXT, updated_at TEXT, diagnostics_json TEXT
            )
        """)
        conn.execute("""
            INSERT INTO executor_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "ETHUSDT", "5", "Buy", "EXITED", -1.0, 1.2, "exit_stop_loss_hit",
            "2026-06-04T11:00:00+00:00", "2026-06-04T11:00:00+00:00",
            json.dumps({"exit_shadow_exit_r": 0.72, "exit_shadow_actual_r": -1.0, "exit_shadow_delta_r": 1.72}),
        ))
        conn.execute("""
            CREATE TABLE trade_lifecycle_events (
                signal_key TEXT, symbol TEXT, timeframe TEXT, side TEXT, event_type TEXT, status TEXT,
                action TEXT, reason TEXT, price REAL, created_at TEXT, features_json TEXT
            )
        """)
        conn.execute("""
            INSERT INTO trade_lifecycle_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "ETHUSDT|linear|5|CONFIRMED_LONG|Buy", "ETHUSDT", "5", "Buy", "EXECUTOR_SHADOW_EXIT", "SHADOW_EXIT",
            "SHADOW_TRAILING_EXIT", "shadow_trailing_40pct_after_1r_triggered", 107.0, "2026-06-04T10:00:00+00:00", "{}",
        ))

    with _client_for_db(db_path, monkeypatch) as client:
        response = client.get("/api/executor-exit-shadow")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["open_shadow_count"] == 1
    assert payload["summary"]["triggered_open_count"] == 1
    assert payload["summary"]["closed_with_shadow_count"] == 1
    assert payload["summary"]["shadow_delta_r"] == 1.72
    assert payload["open_shadow_trades"][0]["exit_shadow_triggered"] is True
    assert payload["closed_shadow_results"][0]["shadow_exit_r"] == 0.72
    assert payload["shadow_events"][0]["event_type"] == "EXECUTOR_SHADOW_EXIT"
