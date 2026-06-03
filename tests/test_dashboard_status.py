from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

import dashboard.server as server_module


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _client_for_db(db_path: Path, monkeypatch) -> TestClient:
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    return TestClient(server_module.create_app())


def test_status_executor_online_when_recent_executor_watch_exists(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE trade_lifecycle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                event_type TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO trade_lifecycle_events (signal_key, symbol, event_type, created_at)
            VALUES (?, ?, ?, ?)
            """,
            ("BTCUSDT|15", "BTCUSDT", "EXECUTOR_WATCH", _iso(datetime.now(timezone.utc))),
        )

    response = _client_for_db(db_path, monkeypatch).get("/api/status")

    assert response.status_code == 200
    assert response.json()["executor"] == "online"


def test_status_executor_online_when_recent_executor_outcome_updated_at_exists(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE executor_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                state TEXT NOT NULL,
                action TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO executor_outcomes (signal_key, symbol, state, action, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("ETHUSDT|15", "ETHUSDT", "WATCHING", "WATCH", _iso(datetime.now(timezone.utc))),
        )

    response = _client_for_db(db_path, monkeypatch).get("/api/status")

    assert response.status_code == 200
    assert response.json()["executor"] == "online"


def test_status_open_trades_counts_entered_and_hold_executor_outcomes(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    now = _iso(datetime.now(timezone.utc))
    rows = [
        ("entered", "BTCUSDT", "ENTERED", "ENTER", now),
        ("hold", "ETHUSDT", "WATCHING", "HOLD", now),
        ("breakeven", "SOLUSDT", "PROTECT_BREAKEVEN", "HOLD", now),
        ("exited_hold", "XRPUSDT", "EXITED", "HOLD", now),
        ("watch", "ADAUSDT", "WATCHING", "WATCH", now),
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE executor_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                state TEXT NOT NULL,
                action TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO executor_outcomes (signal_key, symbol, state, action, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )

    response = _client_for_db(db_path, monkeypatch).get("/api/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["executor"] == "online"
    assert payload["open_trades"] == 3


def test_status_closed_trades_today_counts_executor_trades_exit_time(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE executor_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL UNIQUE,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                exit_time TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO executor_trades (trade_key, signal_key, symbol, exit_time)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("today", "BTCUSDT|15", "BTCUSDT", _iso(today)),
                ("yesterday", "ETHUSDT|15", "ETHUSDT", _iso(yesterday)),
                ("open", "SOLUSDT|15", "SOLUSDT", None),
            ],
        )

    response = _client_for_db(db_path, monkeypatch).get("/api/status")

    assert response.status_code == 200
    assert response.json()["closed_trades_today"] == 1
