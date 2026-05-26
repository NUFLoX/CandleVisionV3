from __future__ import annotations

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
            "A|linear|5|PRE_IMPULSE_ZONE|Buy", "A", "linear", "5", "orderflow", "PRE_IMPULSE_ZONE", "Buy",
            7.0, 7.0, 7.0, 1.0, 0.9, 1.1, 1.2,
            '["sell_pressure_absorbed"]', '["sell_pressure_absorbed","close_in_upper_half"]', '{}',
            "2026-01-01T00:00:00+00:00", "2026-01-01T00:01:00+00:00", 1, "TP1", "TP1", "2026-01-01T00:02:00+00:00", 5.0, None, None, 2.5, -0.6,
        ),
        (
            "B|linear|15|ACCUMULATION_WATCH|Buy", "B", "linear", "15", "orderflow", "ACCUMULATION_WATCH", "Buy",
            4.0, 4.0, 4.0, 2.0, 1.8, 2.2, 2.3,
            '["high_turnover_low_displacement"]', '["high_turnover_low_displacement"]', '{}',
            "2026-01-01T00:00:00+00:00", "2026-01-01T00:01:00+00:00", 1, "SL", "SL", "2026-01-01T00:02:00+00:00", None, None, 6.0, 1.0, -3.0,
        ),
        (
            "C|spot|1|BREAKOUT_PRESSURE|Buy", "C", "spot", "1", "scout", "BREAKOUT_PRESSURE", "Buy",
            11.0, 11.0, 11.0, 3.0, 2.7, 3.3, 3.6,
            '["near_breakout"]', '["near_breakout"]', '{}',
            "2026-01-01T00:00:00+00:00", "2026-01-01T00:01:00+00:00", 1, "PENDING", "PENDING", "2026-01-01T00:02:00+00:00", None, None, None, 0.8, -0.4,
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


def test_signal_performance_report_sections(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    _seed_db(db_path)

    report = subprocess.run(
        [sys.executable, "tools/signal_performance_report.py", "--db", str(db_path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    assert "=== REASON REPORT ===" in report
    assert "=== SCORE BUCKET REPORT ===" in report
    assert "=== TIMEFRAME REPORT ===" in report
    assert "=== KIND REPORT ===" in report
    assert "=== SOURCE REPORT ===" in report
    assert "sell_pressure_absorbed" in report
    assert "PRE_IMPULSE_ZONE" in report
    assert "orderflow" in report
