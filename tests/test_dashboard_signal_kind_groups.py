from __future__ import annotations

import asyncio
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from fastapi.testclient import TestClient

import dashboard.server as server_module


@pytest.fixture(autouse=True)
def _disable_dashboard_live_refresh(monkeypatch):
    async def idle_refresh_loop(*_args, **_kwargs):
        while True:
            await asyncio.sleep(3600)

    monkeypatch.setattr(server_module, "_live_refresh_loop", idle_refresh_loop)


def _init_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            source TEXT NOT NULL,
            kind TEXT NOT NULL,
            side TEXT NOT NULL,
            score_first REAL NOT NULL,
            score_last REAL NOT NULL,
            score_max REAL NOT NULL,
            entry REAL NOT NULL,
            stop_loss REAL NOT NULL,
            take_profit_1 REAL NOT NULL,
            take_profit_2 REAL NOT NULL,
            reasons_first TEXT NOT NULL,
            reasons_last TEXT NOT NULL,
            meta TEXT NOT NULL,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            repeat_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            outcome TEXT,
            max_gain_pct REAL,
            max_drawdown_pct REAL
        )
        """
    )
    rows = [
        (
            "BTCUSDT|linear|15|ACCUMULATION_WATCH|Buy",
            "BTCUSDT",
            "linear",
            "15",
            "orderflow",
            "ACCUMULATION_WATCH",
            "Buy",
            8.0,
            9.0,
            11.0,
            100.0,
            95.0,
            110.0,
            120.0,
            "[]",
            "[]",
            "{}",
            "2026-05-01T00:00:00+00:00",
            "2026-05-02T00:00:00+00:00",
            3,
            "WATCHING",
            "TP2",
            5.0,
            -1.0,
        ),
        (
            "ETHUSDT|linear|15|ACCUMULATION_WATCH|Buy",
            "ETHUSDT",
            "linear",
            "15",
            "orderflow",
            "ACCUMULATION_WATCH",
            "Buy",
            7.0,
            7.0,
            8.0,
            100.0,
            95.0,
            110.0,
            120.0,
            "[]",
            "[]",
            "{}",
            "2026-05-01T00:00:00+00:00",
            "2026-05-02T00:00:00+00:00",
            1,
            "WATCHING",
            "SL",
            2.0,
            -3.0,
        ),
        (
            "SOLUSDT|linear|5|BREAKOUT_PRESSURE|Buy",
            "SOLUSDT",
            "linear",
            "5",
            "scanner",
            "BREAKOUT_PRESSURE",
            "Buy",
            10.0,
            12.0,
            13.0,
            100.0,
            95.0,
            110.0,
            120.0,
            "[]",
            "[]",
            "{}",
            "2026-05-01T00:00:00+00:00",
            "2026-05-02T00:00:00+00:00",
            2,
            "CONFIRMED_LONG",
            None,
            6.0,
            -0.5,
        ),
        (
            "XRPUSDT|linear|60|UNEXPECTED_KIND|Buy",
            "XRPUSDT",
            "linear",
            "60",
            "scanner",
            "UNEXPECTED_KIND",
            "Buy",
            4.0,
            4.0,
            4.0,
            100.0,
            95.0,
            110.0,
            120.0,
            "[]",
            "[]",
            "{}",
            "2026-05-01T00:00:00+00:00",
            "2026-05-02T00:00:00+00:00",
            1,
            "EXPIRED",
            None,
            1.0,
            -2.0,
        ),
    ]
    conn.executemany(
        """
        INSERT INTO signals (
            signal_key,symbol,market,timeframe,source,kind,side,
            score_first,score_last,score_max,entry,stop_loss,take_profit_1,take_profit_2,
            reasons_first,reasons_last,meta,first_seen,last_seen,repeat_count,status,outcome,max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def test_signal_kind_groups_endpoint_returns_grouped_stats(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-kind-groups")

    assert response.status_code == 200
    payload = response.json()
    groups = payload["groups"]
    by_kind = {row["kind"]: row for row in groups}

    watch = by_kind["ACCUMULATION_WATCH"]
    assert watch["signal_family"] == "HIGH_POTENTIAL_ACCUMULATION"
    assert watch["signal_focus_group"] == "HIGH_POTENTIAL"
    assert watch["timeframe"] == "15"
    assert watch["source"] == "orderflow"
    assert watch["total"] == 2
    assert watch["tp2"] == 1
    assert watch["sl"] == 1
    assert watch["expired"] == 0
    assert watch["tp2_rate_closed_pct"] == 50.0
    assert watch["avg_score_last"] == 8.0
    assert watch["avg_score_max"] == 9.5
    assert watch["avg_max_gain_pct"] == 3.5
    assert watch["avg_max_drawdown_pct"] == -2.0

    breakout = by_kind["BREAKOUT_PRESSURE"]
    assert breakout["signal_family"] == "EXECUTION_STABLE_BREAKOUT"
    assert breakout["signal_focus_group"] == "EXECUTION_STABLE"
    assert breakout["confirmed"] == 1

    other = by_kind["UNEXPECTED_KIND"]
    assert other["signal_family"] == "OTHER"
    assert other["signal_focus_group"] == "OTHER"
    assert other["expired"] == 1

    assert [row["kind"] for row in payload["focus_groups"]["HIGH_POTENTIAL"]] == ["ACCUMULATION_WATCH"]
    assert [row["kind"] for row in payload["focus_groups"]["EXECUTION_STABLE"]] == ["BREAKOUT_PRESSURE"]
    assert [row["kind"] for row in payload["focus_groups"]["OTHER"]] == ["UNEXPECTED_KIND"]


def test_signal_kind_groups_endpoint_empty_when_db_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", tmp_path / "missing.db")
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", tmp_path / "missing_reports")

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-kind-groups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["groups"] == []
    assert payload["focus_groups"] == {"HIGH_POTENTIAL": [], "EXECUTION_STABLE": [], "EXPERIMENTAL": [], "OTHER": []}
    assert payload["profit_potential"]["available"] is False
    assert payload["profit_potential"]["by_kind"] == {}
    assert all(row["profit_potential"] is None for row in payload["profit_potential"]["key_kinds"])
    assert payload["high_potential_focus"]["by_kind"] == []
    assert payload["high_potential_focus"]["high_potential_summary"] == []
    assert payload["high_potential_focus"]["profit_potential"]["available"] is False


def test_signal_kind_groups_endpoint_empty_when_signals_table_missing(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(db_path).close()
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", tmp_path / "missing_reports")

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-kind-groups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["groups"] == []
    assert payload["focus_groups"] == {"HIGH_POTENTIAL": [], "EXECUTION_STABLE": [], "EXPERIMENTAL": [], "OTHER": []}
    assert payload["profit_potential"]["available"] is False
    assert payload["profit_potential"]["by_kind"] == {}
    assert all(row["profit_potential"] is None for row in payload["profit_potential"]["key_kinds"])
    assert payload["high_potential_focus"]["by_kind"] == []
    assert payload["high_potential_focus"]["high_potential_summary"] == []
    assert payload["high_potential_focus"]["profit_potential"]["available"] is False


def test_signal_kind_groups_endpoint_empty_when_signals_table_missing(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(db_path).close()
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", tmp_path / "missing_reports")

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-kind-groups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["groups"] == []
    assert payload["focus_groups"] == {"HIGH_POTENTIAL": [], "EXECUTION_STABLE": [], "EXPERIMENTAL": [], "OTHER": []}
    assert payload["profit_potential"]["available"] is False
    assert payload["high_potential_focus"]["profit_potential"]["available"] is False


def test_high_potential_focus_endpoint_returns_required_kinds_and_recommendations(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.executemany(
        """
        INSERT INTO signals (
            signal_key,symbol,market,timeframe,source,kind,side,
            score_first,score_last,score_max,entry,stop_loss,take_profit_1,take_profit_2,
            reasons_first,reasons_last,meta,first_seen,last_seen,repeat_count,status,outcome,max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                "ADAUSDT|linear|15|ABSORPTION_ZONE|Buy",
                "ADAUSDT",
                "linear",
                "15",
                "orderflow",
                "ABSORPTION_ZONE",
                "Buy",
                6.0,
                7.0,
                9.0,
                100.0,
                95.0,
                110.0,
                120.0,
                "[]",
                "[]",
                "{}",
                "2026-05-01T00:00:00+00:00",
                "2026-05-02T00:00:00+00:00",
                1,
                "WATCHING",
                "EXPIRED",
                4.0,
                -1.0,
            ),
            (
                "DOGEUSDT|linear|5|PRE_IMPULSE_ZONE|Buy",
                "DOGEUSDT",
                "linear",
                "5",
                "orderflow",
                "PRE_IMPULSE_ZONE",
                "Buy",
                6.0,
                6.5,
                8.5,
                100.0,
                95.0,
                110.0,
                120.0,
                "[]",
                "[]",
                "{}",
                "2026-05-01T00:00:00+00:00",
                "2026-05-02T00:00:00+00:00",
                1,
                "WATCHING",
                "SL",
                5.0,
                -2.0,
            ),
        ],
    )
    before_rows = conn.execute("SELECT signal_key, kind, status, outcome FROM signals ORDER BY signal_key").fetchall()
    conn.commit()
    conn.close()
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/high-potential-focus")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) >= {
        "high_potential_summary",
        "by_kind",
        "by_timeframe",
        "by_symbol",
        "by_kind_timeframe",
        "management_recommendations",
    }
    assert [row["kind"] for row in payload["by_kind"]] == ["ACCUMULATION_WATCH", "ABSORPTION_ZONE", "PRE_IMPULSE_ZONE"]
    recommendations = {row["kind"]: row["recommended_management"] for row in payload["by_kind"]}
    assert recommendations["ACCUMULATION_WATCH"] in {"priority_high_potential", "breakeven_first_trailing_candidate"}
    assert recommendations["ABSORPTION_ZONE"] == "extend_watch_window_or_wait_for_confirmation"
    assert recommendations["PRE_IMPULSE_ZONE"] == "breakeven_first_trailing_candidate"

    conn = sqlite3.connect(str(db_path))
    after_rows = conn.execute("SELECT signal_key, kind, status, outcome FROM signals ORDER BY signal_key").fetchall()
    conn.close()
    assert after_rows == before_rows
