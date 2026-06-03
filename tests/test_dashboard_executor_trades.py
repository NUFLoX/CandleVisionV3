from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

import dashboard.server as server_module


def _client_for_db(db_path: Path, monkeypatch) -> TestClient:
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    return TestClient(server_module.create_app())


def _init_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE executor_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                side TEXT,
                state TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT,
                entry_price REAL,
                current_sl REAL,
                exit_price REAL,
                max_gain_r REAL,
                max_drawdown_r REAL,
                bars_in_trade INTEGER,
                updated_at TEXT,
                created_at TEXT,
                diagnostics_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO signals (signal_key, symbol, timeframe)
            VALUES (?, ?, ?)
            """,
            ("ENAUSDT|linear|15|PRE_IMPULSE_ZONE|Buy", "ENAUSDT", "15"),
        )
        conn.executemany(
            """
            INSERT INTO executor_outcomes (
                signal_key, symbol, side, state, action, reason, entry_price, current_sl,
                exit_price, max_gain_r, max_drawdown_r, bars_in_trade, updated_at, created_at, diagnostics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "ENAUSDT|linear|15|PRE_IMPULSE_ZONE|Buy",
                    "ENAUSDT",
                    "Buy",
                    "ENTERED",
                    "HOLD",
                    "paper position open",
                    0.322,
                    0.301,
                    None,
                    1.25,
                    -0.35,
                    7,
                    "2026-06-03T12:10:00+00:00",
                    "2026-06-03T12:00:00+00:00",
                    json.dumps(
                        {
                            "executor_entry_time": "2026-06-03T12:01:00+00:00",
                            "executor_initial_sl": 0.3,
                            "breakeven_time": "2026-06-03T12:05:00+00:00",
                            "signal_kind": "PRE_IMPULSE_ZONE",
                            "signal_family": "HIGH_POTENTIAL_PRE_IMPULSE",
                            "signal_focus_group": "HIGH_POTENTIAL",
                        }
                    ),
                ),
                (
                    "SOLUSDT|linear|5|ABSORPTION_ZONE|Buy",
                    "SOLUSDT",
                    "Buy",
                    "PROTECT_BREAKEVEN",
                    "HOLD",
                    "protected",
                    150.0,
                    151.0,
                    None,
                    0.75,
                    -0.15,
                    4,
                    "2026-06-03T12:09:00+00:00",
                    "2026-06-03T12:02:00+00:00",
                    json.dumps({"signal_kind": "ABSORPTION_ZONE", "signal_focus_group": "HIGH_POTENTIAL"}),
                ),
                (
                    "XRPUSDT|linear|15|BREAKOUT_PRESSURE|Buy",
                    "XRPUSDT",
                    "Buy",
                    "EXITED",
                    "EXIT",
                    "closed",
                    0.5,
                    0.49,
                    0.55,
                    2.0,
                    -0.2,
                    9,
                    "2026-06-03T12:11:00+00:00",
                    "2026-06-03T11:00:00+00:00",
                    "{}",
                ),
            ],
        )


def test_executor_active_trades_returns_entered_hold_row_with_joined_timeframe_and_diagnostics(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)

    with _client_for_db(db_path, monkeypatch) as client:
        response = client.get("/api/executor-active-trades?limit=10")

    assert response.status_code == 200
    payload = response.json()
    ena = next(row for row in payload["rows"] if row["symbol"] == "ENAUSDT")
    assert ena["state"] == "ENTERED"
    assert ena["action"] == "HOLD"
    assert ena["timeframe"] == "15"
    assert ena["entry_price"] == 0.322
    assert ena["current_sl"] == 0.301
    assert ena["max_gain_r"] == 1.25
    assert ena["max_drawdown_r"] == -0.35
    assert ena["bars_in_trade"] == 7
    assert ena["executor_entry_time"] == "2026-06-03T12:01:00+00:00"
    assert ena["executor_initial_sl"] == 0.3
    assert ena["breakeven_time"] == "2026-06-03T12:05:00+00:00"
    assert ena["signal_kind"] == "PRE_IMPULSE_ZONE"
    assert ena["signal_family"] == "HIGH_POTENTIAL_PRE_IMPULSE"
    assert ena["signal_focus_group"] == "HIGH_POTENTIAL"


def test_executor_active_trades_excludes_exited_rows_and_counts_summary(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)

    with _client_for_db(db_path, monkeypatch) as client:
        response = client.get("/api/executor-active-trades?limit=10")

    assert response.status_code == 200
    payload = response.json()
    symbols = {row["symbol"] for row in payload["rows"]}
    assert symbols == {"ENAUSDT", "SOLUSDT"}
    assert payload["summary"] == {
        "total_open_trades": 2,
        "protect_breakeven_count": 1,
        "entered_count": 1,
        "avg_max_gain_r": 1.0,
        "avg_max_drawdown_r": -0.25,
    }
