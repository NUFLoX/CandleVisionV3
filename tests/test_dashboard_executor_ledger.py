from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

import dashboard.server as server_module


def _client_for_db(db_path: Path, monkeypatch) -> TestClient:
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    return TestClient(server_module.create_app())


def _init_ledger_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                kind TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE executor_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL UNIQUE,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT,
                side TEXT,
                state TEXT,
                entry_price REAL,
                exit_price REAL,
                initial_sl REAL,
                final_sl REAL,
                current_sl REAL,
                entry_time TEXT,
                exit_time TEXT,
                exit_reason TEXT,
                r_result REAL,
                max_gain_r REAL,
                max_drawdown_r REAL,
                bars_in_trade INTEGER,
                duration_minutes REAL,
                moved_to_breakeven INTEGER,
                breakeven_time TEXT,
                diagnostics_json TEXT,
                updated_at TEXT
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
        conn.executemany(
            "INSERT INTO signals (signal_key, symbol, timeframe, kind) VALUES (?, ?, ?, ?)",
            [
                ("BTCUSDT|linear|15|PRE_IMPULSE_ZONE|Buy", "BTCUSDT", "15", "PRE_IMPULSE_ZONE"),
                ("ETHUSDT|linear|5|ABSORPTION_ZONE|Buy", "ETHUSDT", "5", "ABSORPTION_ZONE"),
                ("SOLUSDT|linear|1|BREAKOUT_PRESSURE|Buy", "SOLUSDT", "1", "BREAKOUT_PRESSURE"),
                ("ADAUSDT|linear|30|ACCUMULATION_WATCH|Buy", "ADAUSDT", "30", "ACCUMULATION_WATCH"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO executor_trades (
                trade_key, signal_key, symbol, timeframe, side, state, entry_price, exit_price, initial_sl,
                final_sl, current_sl, entry_time, exit_time, exit_reason, r_result, max_gain_r,
                max_drawdown_r, bars_in_trade, duration_minutes, moved_to_breakeven, breakeven_time,
                diagnostics_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "win", "BTCUSDT|linear|15|PRE_IMPULSE_ZONE|Buy", "BTCUSDT", "15", "Buy", "EXITED",
                    100.0, 105.0, 98.0, 101.0, 101.0, "2026-06-04T10:00:00+00:00",
                    datetime.now(timezone.utc).isoformat(), "TAKE_PROFIT", 2.0, 2.5, -0.2, 5, 50.0, 1,
                    "2026-06-04T10:20:00+00:00", json.dumps({"signal_kind": "PRE_IMPULSE_ZONE"}),
                    "2026-06-04T11:00:00+00:00",
                ),
                (
                    "loss", "ETHUSDT|linear|5|ABSORPTION_ZONE|Buy", "ETHUSDT", "5", "Buy", "EXITED",
                    50.0, 49.0, 49.0, 49.0, 49.0, "2026-06-03T10:00:00+00:00",
                    "2026-06-03T10:30:00+00:00", "STOP_LOSS", -1.0, 0.3, -1.0, 3, 30.0, 0,
                    None, "not-json", "2026-06-03T10:30:00+00:00",
                ),
                (
                    "flat", "SOLUSDT|linear|1|BREAKOUT_PRESSURE|Buy", "SOLUSDT", "1", "Buy", "EXITED",
                    20.0, 20.0, 19.5, 20.0, 20.0, "2026-06-02T10:00:00+00:00",
                    "2026-06-02T10:05:00+00:00", "BREAKEVEN", 0.0, 0.7, -0.1, 1, 5.0, 1,
                    "2026-06-02T10:03:00+00:00", json.dumps({"signal_focus_group": "CUSTOM_FOCUS"}),
                    "2026-06-02T10:05:00+00:00",
                ),
            ],
        )
        conn.executemany(
            """
            INSERT INTO executor_outcomes (
                signal_key, symbol, side, state, action, reason, entry_price, current_sl, exit_price,
                max_gain_r, max_drawdown_r, bars_in_trade, updated_at, created_at, diagnostics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "ADAUSDT|linear|30|ACCUMULATION_WATCH|Buy", "ADAUSDT", "Buy", "ENTERED", "HOLD",
                    "paper open", 1.0, 0.95, None, 0.5, -0.2, 4, "2026-06-04T12:00:00+00:00",
                    "2026-06-04T11:50:00+00:00",
                    json.dumps({"executor_entry_time": "2026-06-04T11:51:00+00:00", "executor_initial_sl": 0.94, "signal_family": "WATCH"}),
                ),
                ("watch", "XRPUSDT", "Buy", "TRADE_WATCH", "WATCH", "watch", None, None, None, 0, 0, 0, "2026-06-04T12:01:00+00:00", "2026-06-04T12:00:00+00:00", "{}"),
                ("exit", "DOGEUSDT", "Buy", "EXITED", "EXIT", "exit", 1, 1, 1, 0, 0, 1, "2026-06-04T12:02:00+00:00", "2026-06-04T12:00:00+00:00", "{}"),
            ],
        )



def _insert_executor_outcome(
    db_path: Path,
    *,
    signal_key: str,
    symbol: str,
    side: str = "Buy",
    state: str = "ENTERED",
    action: str = "HOLD",
    entry_price: float = 1.0,
    current_sl: float = 0.95,
    max_gain_r: float = 0.0,
    diagnostics_json: dict | None = None,
    updated_at: str = "2026-06-04T12:30:00+00:00",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO signals (signal_key, symbol, timeframe, kind) VALUES (?, ?, ?, ?)",
            (signal_key, symbol, "5", "PRE_IMPULSE_ZONE"),
        )
        conn.execute(
            """
            INSERT INTO executor_outcomes (
                signal_key, symbol, side, state, action, reason, entry_price, current_sl, exit_price,
                max_gain_r, max_drawdown_r, bars_in_trade, updated_at, created_at, diagnostics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_key, symbol, side, state, action, "test", entry_price, current_sl, None,
                max_gain_r, -0.1, 2, updated_at, "2026-06-04T12:00:00+00:00",
                json.dumps(diagnostics_json or {}),
            ),
        )


def test_executor_ledger_missing_database_returns_safe_empty_payload(tmp_path: Path, monkeypatch) -> None:
    response = _client_for_db(tmp_path / "missing.db", monkeypatch).get("/api/executor-ledger")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_closed_trades"] == 0
    assert payload["summary"]["total_open_trades"] == 0
    assert payload["open_trades"] == []
    assert payload["closed_trades"] == []
    assert payload["exit_reasons"] == []


def test_executor_ledger_missing_tables_returns_safe_empty_payload(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(db_path).close()

    response = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_closed_trades"] == 0
    assert payload["open_trades"] == []
    assert payload["closed_trades"] == []
    assert payload["exit_reasons"] == []


def test_executor_ledger_closed_trades_are_summarized_correctly(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)

    response = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger")

    assert response.status_code == 200
    summary = response.json()["summary"]
    assert summary["total_closed_trades"] == 3
    assert summary["wins"] == 1
    assert summary["losses"] == 1
    assert summary["breakeven_or_flat"] == 1
    assert summary["net_r"] == 1.0
    assert summary["avg_r"] == 0.3333
    assert summary["win_rate"] == 0.5
    assert summary["profit_factor"] == 2.0
    assert summary["breakeven_moves"] == 2


def test_executor_ledger_open_outcomes_filters_active_rows(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()

    assert [row["symbol"] for row in payload["open_trades"]] == ["ADAUSDT"]
    row = payload["open_trades"][0]
    assert row["state"] == "ENTERED"
    assert row["action"] == "HOLD"
    assert row["timeframe"] == "30"
    assert row["executor_entry_time"] == "2026-06-04T11:51:00+00:00"
    assert row["executor_initial_sl"] == 0.94


def test_executor_ledger_exit_reason_breakdown_is_calculated_correctly(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    by_reason = {row["exit_reason"]: row for row in payload["exit_reasons"]}

    assert by_reason["TAKE_PROFIT"]["total"] == 1
    assert by_reason["TAKE_PROFIT"]["wins"] == 1
    assert by_reason["TAKE_PROFIT"]["net_r"] == 2.0
    assert by_reason["STOP_LOSS"]["losses"] == 1
    assert by_reason["STOP_LOSS"]["avg_r"] == -1.0
    assert by_reason["BREAKEVEN"]["net_r"] == 0.0


def test_executor_ledger_diagnostics_json_fields_are_parsed_safely(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    loss = next(row for row in payload["closed_trades"] if row["trade_key"] == "loss")
    flat = next(row for row in payload["closed_trades"] if row["trade_key"] == "flat")

    assert loss["signal_kind"] == "ABSORPTION_ZONE"
    assert flat["signal_focus_group"] == "CUSTOM_FOCUS"


def test_executor_ledger_includes_signal_taxonomy_fields_when_possible(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    win = next(row for row in payload["closed_trades"] if row["trade_key"] == "win")
    open_row = payload["open_trades"][0]

    assert win["signal_kind"] == "PRE_IMPULSE_ZONE"
    assert win["signal_family"]
    assert win["signal_focus_group"]
    assert open_row["signal_kind"] == "ACCUMULATION_WATCH"
    assert open_row["signal_focus_group"]


def test_executor_ledger_open_buy_trade_hides_stale_breakeven_time(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)
    old_breakeven_time = "2026-06-02T15:08:53+00:00"
    _insert_executor_outcome(
        db_path,
        signal_key="XLMUSDT|linear|5|PRE_IMPULSE_ZONE|Buy",
        symbol="XLMUSDT",
        state="ENTERED",
        entry_price=1.0,
        current_sl=0.95,
        max_gain_r=0.3933,
        diagnostics_json={"breakeven_time": old_breakeven_time},
    )

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    row = next(row for row in payload["open_trades"] if row["symbol"] == "XLMUSDT")

    assert row["breakeven_time"] == old_breakeven_time
    assert row["breakeven_active"] is False
    assert row["breakeven_display_time"] is None
    assert row["stale_breakeven_time"] is True


def test_executor_ledger_open_buy_protect_breakeven_displays_breakeven_time(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)
    breakeven_time = "2026-06-04T12:05:00+00:00"
    _insert_executor_outcome(
        db_path,
        signal_key="BNBUSDT|linear|5|PRE_IMPULSE_ZONE|Buy",
        symbol="BNBUSDT",
        state="PROTECT_BREAKEVEN",
        entry_price=1.0,
        current_sl=1.001,
        diagnostics_json={"breakeven_time": breakeven_time},
    )

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    row = next(row for row in payload["open_trades"] if row["symbol"] == "BNBUSDT")

    assert row["breakeven_active"] is True
    assert row["breakeven_display_time"] == breakeven_time
    assert row["stale_breakeven_time"] is False


def test_executor_ledger_open_buy_entered_with_sl_above_entry_is_breakeven_active(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)
    _insert_executor_outcome(
        db_path,
        signal_key="LINKUSDT|linear|5|PRE_IMPULSE_ZONE|Buy",
        symbol="LINKUSDT",
        state="ENTERED",
        entry_price=1.0,
        current_sl=1.001,
        diagnostics_json={},
    )

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    row = next(row for row in payload["open_trades"] if row["symbol"] == "LINKUSDT")

    assert row["breakeven_active"] is True
    assert row["breakeven_display_time"] is None
    assert row["stale_breakeven_time"] is False


def test_executor_ledger_closed_trades_keep_moved_to_breakeven_and_breakeven_time(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_ledger_db(db_path)

    payload = _client_for_db(db_path, monkeypatch).get("/api/executor-ledger").json()
    win = next(row for row in payload["closed_trades"] if row["trade_key"] == "win")
    loss = next(row for row in payload["closed_trades"] if row["trade_key"] == "loss")

    assert win["moved_to_breakeven"] is True
    assert win["breakeven_time"] == "2026-06-04T10:20:00+00:00"
    assert loss["moved_to_breakeven"] is False
    assert loss["breakeven_time"] is None
