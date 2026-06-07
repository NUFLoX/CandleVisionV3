from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from dashboard import server
from dashboard.server import _read_executor_regime_performance, app


def create_schema(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
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
            exit_time TEXT,
            exit_reason TEXT,
            r_result REAL,
            max_gain_r REAL,
            max_drawdown_r REAL,
            diagnostics_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE trade_lifecycle_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT,
            side TEXT,
            event_type TEXT NOT NULL,
            status TEXT,
            action TEXT,
            reason TEXT,
            price REAL,
            btc_regime TEXT,
            market_regime TEXT,
            features_json TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE executor_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT,
            state TEXT,
            action TEXT,
            reason TEXT,
            entry_price REAL,
            price REAL,
            diagnostics_json TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )
    return conn


def insert_trade(
    conn: sqlite3.Connection,
    *,
    trade_key: str,
    signal_key: str,
    r_result: float,
    diagnostics: dict | None = None,
    max_gain_r: float = 0.0,
    max_drawdown_r: float = 0.0,
) -> None:
    conn.execute(
        """
        INSERT INTO executor_trades (
            trade_key, signal_key, symbol, timeframe, side, state, exit_time, exit_reason,
            r_result, max_gain_r, max_drawdown_r, diagnostics_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'EXITED', ?, 'take_profit', ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_key,
            signal_key,
            "ETHUSDT",
            "5",
            "Buy",
            f"2026-06-01T00:0{len(trade_key)}:00+00:00",
            r_result,
            max_gain_r,
            max_drawdown_r,
            json.dumps(diagnostics or {}),
            "2026-06-01T00:00:00+00:00",
            "2026-06-01T00:10:00+00:00",
        ),
    )


def test_regime_performance_uses_executor_trade_diagnostics_and_profit_factor(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    conn = create_schema(db_path)
    insert_trade(
        conn,
        trade_key="t1",
        signal_key="s1",
        r_result=2.0,
        diagnostics={"btc_regime": "BTC_BULLISH", "market_regime": "RISK_ON"},
        max_gain_r=2.5,
        max_drawdown_r=-0.2,
    )
    insert_trade(
        conn,
        trade_key="t2",
        signal_key="s2",
        r_result=-1.0,
        diagnostics={"btc_regime": "BTC_BULLISH", "market_regime": "RISK_ON"},
        max_gain_r=0.3,
        max_drawdown_r=-1.0,
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(server, "SIGNALS_DB_PATH", db_path)

    payload = _read_executor_regime_performance()

    bullish = next(row for row in payload["by_btc_regime"] if row["btc_regime"] == "BTC_BULLISH")
    assert bullish["total_trades"] == 2
    assert bullish["wins"] == 1
    assert bullish["losses"] == 1
    assert bullish["net_r"] == 1.0
    assert bullish["profit_factor"] == 2.0
    assert bullish["avg_max_gain_r"] == 1.4
    assert bullish["avg_max_drawdown_r"] == -0.6


def test_regime_performance_falls_back_to_latest_lifecycle_event(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    conn = create_schema(db_path)
    insert_trade(conn, trade_key="t1", signal_key="s1", r_result=1.0, diagnostics={"other": "value"})
    conn.execute(
        """
        INSERT INTO trade_lifecycle_events (
            signal_key, symbol, timeframe, side, event_type, action, reason, price,
            btc_regime, market_regime, features_json, created_at
        ) VALUES ('s1', 'ETHUSDT', '5', 'Buy', 'EXECUTOR_ENTRY', 'ENTER', 'ok', 100, 'BTC_BEARISH', 'RISK_OFF', '{}', '2026-06-01T00:20:00+00:00')
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(server, "SIGNALS_DB_PATH", db_path)

    payload = _read_executor_regime_performance()

    bearish = next(row for row in payload["by_btc_regime"] if row["btc_regime"] == "BTC_BEARISH")
    risk_off = next(row for row in payload["by_market_regime"] if row["market_regime"] == "RISK_OFF")
    assert bearish["total_trades"] == 1
    assert risk_off["total_trades"] == 1


def test_regime_performance_unknown_bucket_when_no_regime_info(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    conn = create_schema(db_path)
    insert_trade(conn, trade_key="t1", signal_key="s1", r_result=0.0, diagnostics={})
    conn.commit()
    conn.close()
    monkeypatch.setattr(server, "SIGNALS_DB_PATH", db_path)

    payload = _read_executor_regime_performance()

    unknown = next(row for row in payload["by_btc_regime"] if row["btc_regime"] == "UNKNOWN")
    assert unknown["total_trades"] == 1
    assert unknown["breakeven_or_flat"] == 1
    assert payload["summary"]["unknown_btc_regime_trades"] == 1


def test_blocked_entry_counting_from_executor_outcomes(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    conn = create_schema(db_path)
    conn.execute(
        """
        INSERT INTO executor_outcomes (
            signal_key, symbol, side, state, action, reason, entry_price, price,
            diagnostics_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "BTCUSDT|linear|5|BREAKOUT_PRESSURE|Buy",
            "BTCUSDT",
            "Buy",
            "BLOCKED",
            "BLOCK",
            "entry_blocked_market_regime",
            50000.0,
            50001.0,
            json.dumps({"btc_regime": "BTC_DUMP_RISK", "market_regime": "RISK_OFF", "timeframe": "5"}),
            "2026-06-01T00:00:00+00:00",
            "2026-06-01T00:01:00+00:00",
        ),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(server, "SIGNALS_DB_PATH", db_path)

    payload = _read_executor_regime_performance()

    assert payload["blocked_entries"]["total_blocked"] == 1
    assert payload["summary"]["total_blocked_by_market_regime"] == 1
    assert next(row for row in payload["blocked_entries"]["by_market_regime"] if row["market_regime"] == "RISK_OFF")["total"] == 1
    assert payload["blocked_entries"]["latest"][0]["reason"] == "entry_blocked_market_regime"
    assert payload["blocked_entries"]["latest"][0]["btc_regime"] == "BTC_DUMP_RISK"


def test_executor_regime_performance_api_returns_expected_keys(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    conn = create_schema(db_path)
    insert_trade(conn, trade_key="t1", signal_key="s1", r_result=1.0, diagnostics={"btc_regime": "BTC_NEUTRAL", "market_regime": "NEUTRAL"})
    conn.commit()
    conn.close()
    monkeypatch.setattr(server, "SIGNALS_DB_PATH", db_path)

    response = TestClient(app).get("/api/executor-regime-performance")

    assert response.status_code == 200
    payload = response.json()
    assert {"summary", "by_btc_regime", "by_market_regime", "blocked_entries"}.issubset(payload)
    assert {"total_blocked", "by_btc_regime", "by_market_regime", "by_symbol", "by_timeframe", "latest"}.issubset(payload["blocked_entries"])


def test_dashboard_static_contains_executor_regime_panel_title() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "Executor Performance by BTC Regime" in html
