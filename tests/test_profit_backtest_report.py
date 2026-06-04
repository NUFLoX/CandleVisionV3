from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

import dashboard.server as server_module
from tools.profit_backtest_report import (
    BY_KIND_REPORT,
    RAW_REPORT,
    SUMMARY_REPORT,
    generate_profit_backtest_report,
)


def _create_signals_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE signals (
            signal_key TEXT,
            symbol TEXT,
            market TEXT,
            timeframe TEXT,
            source TEXT,
            kind TEXT,
            side TEXT,
            score_first REAL,
            score_last REAL,
            score_max REAL,
            entry REAL,
            stop_loss REAL,
            take_profit_1 REAL,
            take_profit_2 REAL,
            first_seen TEXT,
            last_seen TEXT,
            repeat_count INTEGER,
            status TEXT,
            outcome TEXT,
            max_gain_pct REAL,
            max_drawdown_pct REAL
        )
        """
    )
    rows = [
        (
            "btc-accumulation",
            "BTCUSDT",
            "linear",
            "15",
            "orderflow",
            "ACCUMULATION_WATCH",
            "Buy",
            8.0,
            9.0,
            10.0,
            100.0,
            95.0,
            110.0,
            120.0,
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T01:00:00+00:00",
            2,
            "WATCHING",
            "TP1",
            25.0,
            -2.0,
        ),
        (
            "eth-absorption",
            "ETHUSDT",
            "linear",
            "15",
            "orderflow",
            "ABSORPTION_ZONE",
            "Buy",
            6.0,
            6.5,
            7.0,
            100.0,
            95.0,
            105.0,
            110.0,
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T01:00:00+00:00",
            1,
            "WATCHING",
            None,
            5.0,
            -1.0,
        ),
        (
            "sol-absorption-loss",
            "SOLUSDT",
            "linear",
            "5",
            "scanner",
            "ABSORPTION_ZONE",
            "Buy",
            5.0,
            5.5,
            6.0,
            100.0,
            94.0,
            108.0,
            112.0,
            "2026-05-01T00:00:00+00:00",
            "2026-05-01T01:00:00+00:00",
            1,
            "CLOSED",
            "SL",
            8.0,
            -6.0,
        ),
    ]
    conn.executemany(
        """
        INSERT INTO signals (
            signal_key,symbol,market,timeframe,source,kind,side,score_first,score_last,score_max,
            entry,stop_loss,take_profit_1,take_profit_2,first_seen,last_seen,repeat_count,status,outcome,
            max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_generates_profit_backtest_reports_from_signals_db(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    out_dir = tmp_path / "reports_profit_backtest"
    _create_signals_db(db_path)

    summary = generate_profit_backtest_report(db_path, out_dir, stake_usd=10)

    assert (out_dir / BY_KIND_REPORT).exists()
    assert (out_dir / RAW_REPORT).exists()
    assert (out_dir / SUMMARY_REPORT).exists()
    assert summary["total_signals"] == 3
    assert summary["valid_signals"] == 3

    raw_rows = {row["signal_key"]: row for row in _read_csv(out_dir / RAW_REPORT)}
    assert raw_rows["btc-accumulation"]["potential_profit_usd"] == "2.5"
    assert raw_rows["btc-accumulation"]["hit_10_pct"] == "1"
    assert raw_rows["btc-accumulation"]["hit_20_pct"] == "1"
    assert raw_rows["btc-accumulation"]["hit_50_pct"] == "0"
    assert raw_rows["sol-absorption-loss"]["first_touch_win"] == "0"
    assert raw_rows["sol-absorption-loss"]["first_touch_profit_usd"] == "-0.6"

    by_kind = {row["kind"]: row for row in _read_csv(out_dir / BY_KIND_REPORT)}
    assert set(by_kind) == {"ACCUMULATION_WATCH", "ABSORPTION_ZONE"}
    assert by_kind["ACCUMULATION_WATCH"]["hit_10_pct_share"] == "1"
    assert by_kind["ACCUMULATION_WATCH"]["hit_20_pct_share"] == "1"
    assert by_kind["ACCUMULATION_WATCH"]["hit_50_pct_share"] == "0"
    assert by_kind["ACCUMULATION_WATCH"]["avg_potential_profit_usd"] == "2.5"
    assert by_kind["ACCUMULATION_WATCH"]["first_touch_win_rate"] == "1"
    assert by_kind["ABSORPTION_ZONE"]["hit_10_pct_share"] == "0"
    assert by_kind["ABSORPTION_ZONE"]["first_touch_win_rate"] == "0"


def test_missing_db_writes_empty_reports_and_summary(tmp_path: Path) -> None:
    out_dir = tmp_path / "reports_profit_backtest"

    summary = generate_profit_backtest_report(tmp_path / "missing.db", out_dir, stake_usd=10)

    assert (out_dir / BY_KIND_REPORT).exists()
    assert (out_dir / RAW_REPORT).exists()
    assert (out_dir / SUMMARY_REPORT).exists()
    assert summary["total_signals"] == 0
    assert summary["valid_signals"] == 0
    assert summary["kinds_count"] == 0
    assert "signals database is missing" in summary["notes"]
    assert _read_csv(out_dir / BY_KIND_REPORT) == []
    assert _read_csv(out_dir / RAW_REPORT) == []
    saved = json.loads((out_dir / SUMMARY_REPORT).read_text(encoding="utf-8"))
    assert saved["total_signals"] == 0


def test_dashboard_profit_potential_reads_generated_reports(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    reports_dir = tmp_path / "reports_profit_backtest"
    _create_signals_db(db_path)
    generate_profit_backtest_report(db_path, reports_dir, stake_usd=10)
    monkeypatch.setattr(server_module, "PROFIT_BACKTEST_DIR", reports_dir)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/signal-profit-potential")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert "ACCUMULATION_WATCH" in payload["by_kind"]
    metrics = payload["by_kind"]["ACCUMULATION_WATCH"]
    assert "avg_max_gain_pct" in metrics
    assert "hit_10_pct_share" in metrics
    assert "avg_potential_profit_usd" in metrics
    assert "first_touch_avg_profit_usd" in metrics
    assert metrics["avg_max_gain_pct"] == 25.0
    assert metrics["avg_potential_profit_usd"] == 2.5
