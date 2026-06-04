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


def _init_db(path: Path, *, with_signals: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        if with_signals:
            conn.execute(
                """
                CREATE TABLE signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_key TEXT NOT NULL UNIQUE,
                    symbol TEXT,
                    timeframe TEXT,
                    kind TEXT
                )
                """
            )
        conn.execute(
            """
            CREATE TABLE executor_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL UNIQUE,
                signal_key TEXT,
                symbol TEXT,
                timeframe TEXT,
                side TEXT,
                state TEXT,
                entry_price REAL,
                exit_price REAL,
                entry_time TEXT,
                exit_time TEXT,
                exit_reason TEXT,
                r_result REAL,
                max_gain_r REAL,
                max_drawdown_r REAL,
                duration_minutes REAL,
                moved_to_breakeven INTEGER,
                diagnostics_json TEXT,
                updated_at TEXT
            )
            """
        )


def _insert_trade(
    db_path: Path,
    *,
    trade_key: str,
    signal_key: str = "BTCUSDT|linear|15|PRE_IMPULSE_ZONE|Buy",
    symbol: str = "BTCUSDT",
    timeframe: str | None = "15",
    side: str = "Buy",
    exit_reason: str = "TAKE_PROFIT",
    r_result: float = 1.0,
    max_gain_r: float = 1.0,
    max_drawdown_r: float = -0.1,
    moved_to_breakeven: int = 0,
    diagnostics_json: str | dict = "{}",
    exit_time: str = "2026-06-04T10:00:00+00:00",
) -> None:
    diagnostics_text = json.dumps(diagnostics_json) if isinstance(diagnostics_json, dict) else diagnostics_json
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO executor_trades (
                trade_key, signal_key, symbol, timeframe, side, state, entry_price, exit_price,
                entry_time, exit_time, exit_reason, r_result, max_gain_r, max_drawdown_r,
                duration_minutes, moved_to_breakeven, diagnostics_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'EXITED', 100, 101, '2026-06-04T09:00:00+00:00', ?, ?, ?, ?, ?, 60, ?, ?, ?)
            """,
            (
                trade_key,
                signal_key,
                symbol,
                timeframe,
                side,
                exit_time,
                exit_reason,
                r_result,
                max_gain_r,
                max_drawdown_r,
                moved_to_breakeven,
                diagnostics_text,
                exit_time,
            ),
        )


def test_learning_effectiveness_missing_db_and_tables_return_safe_empty_payload(tmp_path: Path, monkeypatch) -> None:
    missing_response = _client_for_db(tmp_path / "missing.db", monkeypatch).get("/api/learning-effectiveness")
    assert missing_response.status_code == 200
    missing_payload = missing_response.json()
    assert missing_payload["summary"]["total_trades"] == 0
    assert missing_payload["summary"]["learning_status"] == "insufficient_data"
    assert missing_payload["windows"] == []
    assert missing_payload["recent_trades"] == []

    empty_db = tmp_path / "empty.db"
    sqlite3.connect(empty_db).close()
    empty_response = _client_for_db(empty_db, monkeypatch).get("/api/learning-effectiveness")
    assert empty_response.status_code == 200
    assert empty_response.json()["summary"]["total_trades"] == 0


def test_learning_effectiveness_summary_metrics(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)
    _insert_trade(db_path, trade_key="win", r_result=1.0, max_gain_r=1.2, max_drawdown_r=-0.2, exit_time="2026-06-04T10:00:00+00:00")
    _insert_trade(db_path, trade_key="loss", r_result=-1.0, max_gain_r=1.3, max_drawdown_r=-1.0, exit_reason="STOP_LOSS", exit_time="2026-06-04T11:00:00+00:00")
    _insert_trade(db_path, trade_key="be", r_result=0.05, max_gain_r=1.1, max_drawdown_r=-0.1, moved_to_breakeven=1, exit_reason="BREAKEVEN", exit_time="2026-06-04T12:00:00+00:00")

    payload = _client_for_db(db_path, monkeypatch).get("/api/learning-effectiveness").json()
    summary = payload["summary"]

    assert summary["total_trades"] == 3
    assert summary["net_r"] == 0.05
    assert summary["avg_r"] == 0.0167
    assert summary["profit_factor"] == 1.05
    assert summary["avg_giveback_r"] == 1.1833
    assert summary["reached_1r_closed_nonpositive_count"] == 1
    assert summary["breakeven_save_count"] == 1
    assert summary["breakeven_save_r"] == 0.05


def test_learning_effectiveness_giveback_detection(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)
    _insert_trade(db_path, trade_key="giveback", r_result=-1.0, max_gain_r=1.5, max_drawdown_r=-1.0, exit_reason="STOP_LOSS")

    payload = _client_for_db(db_path, monkeypatch).get("/api/learning-effectiveness").json()

    assert payload["recent_trades"][0]["giveback_r"] == 2.5
    assert "large_giveback_after_1r" in {row["pattern"] for row in payload["problem_patterns"]}


def test_learning_effectiveness_kind_and_timeframe_grouping(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO signals (signal_key, symbol, timeframe, kind) VALUES (?, ?, ?, ?)",
            ("ETHUSDT|linear|5|PARSED_KIND|Sell", "ETHUSDT", "5", "JOINED_KIND"),
        )
    _insert_trade(
        db_path,
        trade_key="diagnostic-kind",
        signal_key="BTCUSDT|linear|15|PARSED_KIND|Buy",
        symbol="BTCUSDT",
        timeframe="15",
        r_result=0.5,
        max_gain_r=0.6,
        diagnostics_json={"signal_kind": "DIAGNOSTIC_KIND"},
    )
    _insert_trade(
        db_path,
        trade_key="parsed-tf-kind",
        signal_key="ETHUSDT|linear|5|PARSED_KIND|Sell",
        symbol="ETHUSDT",
        timeframe=None,
        r_result=-0.25,
        max_gain_r=0.2,
        diagnostics_json={},
        exit_time="2026-06-04T11:00:00+00:00",
    )

    payload = _client_for_db(db_path, monkeypatch).get("/api/learning-effectiveness").json()
    by_kind = {row["kind"]: row for row in payload["by_kind"]}
    by_timeframe = {row["timeframe"]: row for row in payload["by_timeframe"]}

    assert by_kind["DIAGNOSTIC_KIND"]["total_trades"] == 1
    assert by_kind["PARSED_KIND"]["total_trades"] == 1
    assert by_timeframe["15"]["total_trades"] == 1
    assert by_timeframe["5"]["total_trades"] == 1


def test_learning_effectiveness_invalid_diagnostics_json_does_not_crash(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path, with_signals=False)
    _insert_trade(db_path, trade_key="bad-json", diagnostics_json="not-json", r_result=0.25, max_gain_r=0.5)

    response = _client_for_db(db_path, monkeypatch).get("/api/learning-effectiveness")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_trades"] == 1
    assert payload["by_kind"][0]["kind"] == "PRE_IMPULSE_ZONE"
