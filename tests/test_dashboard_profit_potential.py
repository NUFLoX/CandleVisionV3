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


def _init_signals_db(path: Path) -> None:
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
    conn.execute(
        """
        INSERT INTO signals (
            signal_key,symbol,market,timeframe,source,kind,side,
            score_first,score_last,score_max,entry,stop_loss,take_profit_1,take_profit_2,
            reasons_first,reasons_last,meta,first_seen,last_seen,repeat_count,status,outcome,max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
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
            1,
            "WATCHING",
            "TP2",
            5.0,
            -1.0,
        ),
    )
    conn.commit()
    conn.close()


def test_signal_profit_potential_endpoint_returns_metrics_when_csv_exists(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports_profit_backtest"
    reports_dir.mkdir()
    (reports_dir / "signal_profit_by_kind.csv").write_text(
        "kind,avg_max_gain_pct,median_max_gain_pct,max_gain_pct,total_potential_profit_usd,avg_potential_profit_usd,hit_10_pct_share,hit_20_pct_share,hit_50_pct_share,first_touch_total_profit_usd,first_touch_avg_profit_usd,first_touch_win_rate\n"
        "ACCUMULATION_WATCH,12.5,11.0,44.0,25.0,2.5,0.8,0.4,0.1,8.0,0.8,0.6\n"
        "BREAKOUT_PRESSURE,7.5,6.0,21.0,10.0,1.0,0.3,0.1,0.0,4.0,0.4,0.5\n",
        encoding="utf-8",
    )
    (reports_dir / "signal_profit_summary.json").write_text('{"stake_usd": 10}', encoding="utf-8")
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", reports_dir)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-profit-potential")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["summary"] == {"stake_usd": 10}
    metrics = payload["by_kind"]["ACCUMULATION_WATCH"]
    assert metrics["avg_max_gain_pct"] == 12.5
    assert metrics["median_max_gain_pct"] == 11.0
    assert metrics["max_gain_pct"] == 44.0
    assert metrics["total_potential_profit_usd"] == 25.0
    assert metrics["avg_potential_profit_usd"] == 2.5
    assert metrics["hit_10_pct_share"] == 0.8
    assert metrics["hit_20_pct_share"] == 0.4
    assert metrics["hit_50_pct_share"] == 0.1
    assert metrics["first_touch_total_profit_usd"] == 8.0
    assert metrics["first_touch_avg_profit_usd"] == 0.8
    assert metrics["first_touch_win_rate"] == 0.6


def test_signal_profit_potential_endpoint_does_not_crash_when_reports_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", tmp_path / "missing_reports")

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-profit-potential")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is False
    assert payload["by_kind"] == {}
    assert all(row["profit_potential"] is None for row in payload["key_kinds"])


def test_signal_intelligence_suppresses_profit_potential_without_signals_db(tmp_path: Path, monkeypatch) -> None:
    reports_dir = tmp_path / "reports_profit_backtest"
    reports_dir.mkdir()
    (reports_dir / "signal_profit_by_kind.csv").write_text(
        "kind,avg_max_gain_pct,avg_potential_profit_usd,hit_10_pct_share\n"
        "ACCUMULATION_WATCH,12.5,2.5,0.8\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", tmp_path / "missing_signals.db")
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", reports_dir)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-kind-groups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["groups"] == []
    assert payload["profit_potential"]["available"] is False
    assert payload["profit_potential"]["by_kind"] == {}
    focus = payload["high_potential_focus"]
    assert focus["profit_potential"]["available"] is False
    assert focus["by_kind"] == []

def test_signal_intelligence_still_works_with_only_signals_db(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_signals_db(db_path)
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", tmp_path / "missing_reports")

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-kind-groups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["profit_potential"]["available"] is False
    assert payload["groups"][0]["kind"] == "ACCUMULATION_WATCH"
    assert payload["groups"][0]["total"] == 1
    assert payload["groups"][0]["profit_potential"] is None
