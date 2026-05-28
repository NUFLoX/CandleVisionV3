from __future__ import annotations

import csv
import sqlite3
import subprocess
import sys
from pathlib import Path


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(path)
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
            outcome_checked_at TEXT,
            time_to_tp1_minutes REAL,
            time_to_tp2_minutes REAL,
            time_to_sl_minutes REAL,
            max_gain_pct REAL,
            max_drawdown_pct REAL
        )
        """
    )
    rows = [
        (
            "A|linear|15|PRE_IMPULSE_ZONE|Buy", "A", "linear", "15", "orderflow", "PRE_IMPULSE_ZONE", "Buy",
            7.0, 9.0, 10.0, 1.0, 0.95, 1.05, 1.10,
            '["sell_pressure_absorbed"]', '["sell_pressure_absorbed","support_defended"]', '{"btc_regime":"BTC_BULLISH"}',
            "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00", 2, "TP2", "TP2", "2026-01-01T02:00:00+00:00", 15.0, 40.0, None, 8.0, -2.0,
        ),
        (
            "B|linear|5|ACCUMULATION_WATCH|Buy", "B", "linear", "5", "orderflow", "ACCUMULATION_WATCH", "Buy",
            6.0, 6.0, 7.0, 2.0, 1.9, 2.1, 2.2,
            'sell_pressure_absorbed|rr_fallback', 'rr_fallback', '{"btc_regime":"BTC_BEARISH"}',
            "2026-01-01T00:00:00+00:00", "2026-01-01T00:20:00+00:00", 1, "SL", "SL", "2026-01-01T00:30:00+00:00", None, None, 20.0, 1.0, -4.0,
        ),
    ]
    conn.executemany(
        """
        INSERT INTO signals (
            signal_key,symbol,market,timeframe,source,kind,side,
            score_first,score_last,score_max,entry,stop_loss,take_profit_1,take_profit_2,
            reasons_first,reasons_last,meta,first_seen,last_seen,repeat_count,status,
            outcome,outcome_checked_at,time_to_tp1_minutes,time_to_tp2_minutes,time_to_sl_minutes,max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def test_watchlist_transition_study_outputs_csv(tmp_path: Path) -> None:
    db = tmp_path / "signals.db"
    out_dir = tmp_path / "reports"
    _seed_db(db)

    subprocess.run(
        [sys.executable, "tools/watchlist_transition_study.py", "--db", str(db), "--out-dir", str(out_dir)],
        check=True,
        capture_output=True,
        text=True,
    )

    expected = [
        out_dir / "watchlist_analysis.csv",
        out_dir / "watchlist_reason_edge.csv",
        out_dir / "watchlist_time_to_move.csv",
        out_dir / "watchlist_timeframe_edge.csv",
        out_dir / "watchlist_btc_regime_edge.csv",
    ]
    for p in expected:
        assert p.exists(), str(p)

    rows = list(csv.DictReader((out_dir / "watchlist_analysis.csv").open("r", encoding="utf-8")))
    assert rows
    assert {r["winner_group"] for r in rows} == {"TP2_WINNER", "SL_LOSER"}
    assert "time_to_0_5R_minutes" in rows[0]


def test_watchlist_reason_edge_report(tmp_path: Path) -> None:
    db = tmp_path / "signals.db"
    out_dir = tmp_path / "reports"
    _seed_db(db)

    subprocess.run(
        [sys.executable, "tools/watchlist_transition_study.py", "--db", str(db), "--out-dir", str(out_dir)],
        check=True,
        capture_output=True,
        text=True,
    )

    rows = list(csv.DictReader((out_dir / "watchlist_reason_edge.csv").open("r", encoding="utf-8")))
    by_reason = {r["reason"]: r for r in rows}
    assert "sell_pressure_absorbed" in by_reason
    assert "rr_fallback" in by_reason
    assert float(by_reason["sell_pressure_absorbed"]["tp_rate"]) >= float(by_reason["rr_fallback"]["tp_rate"])
