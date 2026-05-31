from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from tools.learning_report import (
    DIAGNOSIS_SUMMARY_HEADERS,
    EXECUTOR_BLOCKERS_HEADERS,
    EXECUTOR_TRADES_HEADERS,
    RECOMMENDATIONS_HEADERS,
    SYMBOL_EDGE_HEADERS,
    TIMEFRAME_EDGE_HEADERS,
    generate_learning_report,
)

OUTPUT_FILES = [
    "learning_summary.json",
    "learning_symbol_edge.csv",
    "learning_timeframe_edge.csv",
    "learning_executor_blockers.csv",
    "learning_executor_trades.csv",
    "learning_diagnosis_summary.csv",
    "learning_recommendations.csv",
]


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def create_diagnoses_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE trade_diagnoses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            timeframe TEXT,
            side TEXT,
            outcome TEXT NOT NULL,
            diagnosis TEXT NOT NULL,
            recommendation TEXT,
            r_result REAL,
            max_gain_pct REAL,
            max_drawdown_pct REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def insert_diagnosis(
    conn: sqlite3.Connection,
    *,
    signal_key: str,
    symbol: str,
    timeframe: str,
    outcome: str,
    r_result: float | None = None,
    max_gain_pct: float | None = None,
    max_drawdown_pct: float | None = None,
    recommendation: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO trade_diagnoses (
            signal_key, symbol, timeframe, side, outcome, diagnosis, recommendation,
            r_result, max_gain_pct, max_drawdown_pct, created_at, updated_at
        ) VALUES (?, ?, ?, 'Buy', ?, 'diagnosis', ?, ?, ?, ?, ?, ?)
        """,
        (
            signal_key,
            symbol,
            timeframe,
            outcome,
            recommendation,
            r_result,
            max_gain_pct,
            max_drawdown_pct,
            now_iso(),
            now_iso(),
        ),
    )


def create_executor_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE executor_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            state TEXT NOT NULL,
            action TEXT NOT NULL,
            reason TEXT NOT NULL,
            max_gain_r REAL,
            max_drawdown_r REAL,
            volume_impulse REAL,
            required_volume_impulse REAL,
            buy_flow REAL,
            sell_flow REAL,
            required_buy_flow REAL,
            spread_bps REAL,
            ask_wall_strength REAL,
            bid_wall_strength REAL,
            diagnostics_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def insert_executor(
    conn: sqlite3.Connection,
    *,
    signal_key: str,
    symbol: str,
    action: str,
    reason: str,
    max_gain_r: float = 0.0,
    max_drawdown_r: float = 0.0,
    volume_impulse: float | None = None,
    required_volume_impulse: float | None = None,
    buy_flow: float | None = None,
    sell_flow: float | None = None,
    required_buy_flow: float | None = None,
    spread_bps: float | None = None,
    ask_wall_strength: float | None = None,
    bid_wall_strength: float | None = None,
    diagnostics_json: dict | str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO executor_outcomes (
            signal_key, symbol, side, state, action, reason, max_gain_r, max_drawdown_r,
            volume_impulse, required_volume_impulse, buy_flow, sell_flow, required_buy_flow,
            spread_bps, ask_wall_strength, bid_wall_strength, diagnostics_json, created_at, updated_at
        ) VALUES (?, ?, 'Buy', 'WATCHING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            signal_key,
            symbol,
            action,
            reason,
            max_gain_r,
            max_drawdown_r,
            volume_impulse,
            required_volume_impulse,
            buy_flow,
            sell_flow,
            required_buy_flow,
            spread_bps,
            ask_wall_strength,
            bid_wall_strength,
            json.dumps(diagnostics_json) if isinstance(diagnostics_json, dict) else diagnostics_json,
            now_iso(),
            now_iso(),
        ),
    )



def create_executor_trades_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE executor_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_key TEXT NOT NULL UNIQUE,
            signal_key TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT,
            side TEXT NOT NULL,
            exit_reason TEXT,
            r_result REAL,
            max_gain_r REAL,
            max_drawdown_r REAL,
            moved_to_breakeven INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def insert_executor_trade(
    conn: sqlite3.Connection,
    *,
    trade_key: str,
    signal_key: str,
    symbol: str,
    timeframe: str = "5",
    side: str = "Buy",
    exit_reason: str = "exit_sell_flow_dominance",
    r_result: float | None = None,
    max_gain_r: float | None = None,
    max_drawdown_r: float | None = None,
    moved_to_breakeven: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO executor_trades (
            trade_key, signal_key, symbol, timeframe, side, exit_reason, r_result,
            max_gain_r, max_drawdown_r, moved_to_breakeven, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade_key,
            signal_key,
            symbol,
            timeframe,
            side,
            exit_reason,
            r_result,
            max_gain_r,
            max_drawdown_r,
            moved_to_breakeven,
            now_iso(),
            now_iso(),
        ),
    )

def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_empty_database_does_not_crash_and_writes_all_output_files(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.db"
    sqlite3.connect(db_path).close()

    generate_learning_report(db_path, tmp_path / "reports")

    for filename in OUTPUT_FILES:
        assert (tmp_path / "reports" / filename).exists()
    summary = json.loads((tmp_path / "reports" / "learning_summary.json").read_text(encoding="utf-8"))
    assert summary["total_signals"] == 0
    assert summary["total_diagnoses"] == 0


def test_missing_optional_tables_does_not_crash(tmp_path: Path) -> None:
    db_path = tmp_path / "signals_only.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, symbol TEXT, first_seen TEXT)")
    conn.execute("INSERT INTO signals (symbol, first_seen) VALUES ('BTCUSDT', ?)", (now_iso(),))
    conn.commit()
    conn.close()

    summary = generate_learning_report(db_path, tmp_path / "reports")

    assert summary["total_signals"] == 1
    assert summary["total_diagnoses"] == 0
    assert summary["total_executor_decisions"] == 0


def test_diagnoses_produce_symbol_report(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_diagnoses_table(conn)
    insert_diagnosis(conn, signal_key="eth-1", symbol="ETHUSDT", timeframe="5", outcome="TP1", r_result=1.0)
    insert_diagnosis(conn, signal_key="eth-2", symbol="ETHUSDT", timeframe="5", outcome="SL", r_result=-1.0)
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports", min_sample=2)

    rows = read_csv(tmp_path / "reports" / "learning_symbol_edge.csv")
    assert rows == [
        {
            "symbol": "ETHUSDT",
            "total_diagnoses": "2",
            "tp_count": "1",
            "sl_count": "1",
            "expired_count": "0",
            "ambiguous_count": "0",
            "tp_rate": "0.5",
            "sl_rate": "0.5",
            "avg_r_result": "0",
            "avg_max_gain_pct": "0",
            "avg_max_drawdown_pct": "0",
            "recommendation": "review symbol: high SL rate",
        }
    ]


def test_diagnoses_produce_timeframe_report(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_diagnoses_table(conn)
    insert_diagnosis(conn, signal_key="one", symbol="BTCUSDT", timeframe="15", outcome="TP2", r_result=2.0)
    insert_diagnosis(conn, signal_key="two", symbol="ETHUSDT", timeframe="15", outcome="EXPIRED", r_result=0.0)
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports", min_sample=2)

    rows = read_csv(tmp_path / "reports" / "learning_timeframe_edge.csv")
    assert rows[0]["timeframe"] == "15"
    assert rows[0]["tp_count"] == "1"
    assert rows[0]["expired_count"] == "1"
    assert rows[0]["avg_r_result"] == "1"


def test_executor_outcomes_produce_blocker_report(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_executor_table(conn)
    insert_executor(conn, signal_key="a", symbol="BTCUSDT", action="BLOCK", reason="entry_blocked_volume_impulse", max_gain_r=0.5, max_drawdown_r=-0.2)
    insert_executor(conn, signal_key="b", symbol="ETHUSDT", action="BLOCK", reason="entry_blocked_volume_impulse", max_gain_r=1.5, max_drawdown_r=-0.4)
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports")

    rows = read_csv(tmp_path / "reports" / "learning_executor_blockers.csv")
    assert rows[0]["reason"] == "entry_blocked_volume_impulse"
    assert rows[0]["total"] == "2"
    assert rows[0]["symbols_count"] == "2"
    assert rows[0]["avg_max_gain_r"] == "1"
    assert rows[0]["avg_max_drawdown_r"] == "-0.3"
    for header in (
        "avg_volume_impulse",
        "avg_required_volume_impulse",
        "avg_buy_flow",
        "avg_sell_flow",
        "avg_required_buy_flow",
        "avg_spread_bps",
        "avg_ask_wall_strength",
        "avg_bid_wall_strength",
    ):
        assert header in rows[0]


def test_executor_blocker_report_uses_capped_volume_impulse_for_reporting(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_executor_table(conn)
    insert_executor(
        conn,
        signal_key="huge",
        symbol="BTCUSDT",
        action="WATCH",
        reason="entry_blocked_volume_impulse",
        volume_impulse=31890.0,
        required_volume_impulse=1.2,
        diagnostics_json={
            "volume_impulse_capped": 50.0,
            "volume_impulse_cap": 50.0,
            "volume_impulse_was_capped": True,
            "volume_impulse_ratio_to_required": 31890.0 / 1.2,
            "volume_impulse_ratio_to_required_capped": 50.0 / 1.2,
        },
    )
    insert_executor(
        conn,
        signal_key="normal",
        symbol="ETHUSDT",
        action="WATCH",
        reason="entry_blocked_volume_impulse",
        volume_impulse=1.0,
        required_volume_impulse=1.2,
        diagnostics_json={
            "volume_impulse_capped": 1.0,
            "volume_impulse_cap": 50.0,
            "volume_impulse_was_capped": False,
            "volume_impulse_ratio_to_required": 1.0 / 1.2,
            "volume_impulse_ratio_to_required_capped": 1.0 / 1.2,
        },
    )
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports", min_sample=1)

    rows = read_csv(tmp_path / "reports" / "learning_executor_blockers.csv")
    assert rows[0]["avg_volume_impulse"] == "25.5"
    assert rows[0]["avg_volume_impulse_ratio_to_required"] == "21.25"
    assert rows[0]["volume_impulse_capped_count"] == "1"
    assert rows[0]["volume_impulse_capped_share"] == "0.5"
    assert rows[0]["avg_volume_impulse_raw"] == "15945.5"
    assert rows[0]["max_volume_impulse_raw"] == "31890"
    assert "outliers were capped" in rows[0]["recommendation"]


def test_executor_blocker_report_recommends_mapping_fix_when_missing_default_dominates(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_executor_table(conn)
    for idx in range(4):
        insert_executor(
            conn,
            signal_key=f"missing-{idx}",
            symbol="BTCUSDT",
            action="WATCH",
            reason="entry_blocked_volume_impulse",
            volume_impulse=1.0,
            required_volume_impulse=1.2,
            diagnostics_json={
                "volume_impulse_source": "missing_default",
                "volume_impulse_missing": True,
                "volume_impulse_ratio_to_required": 1.0 / 1.2,
            },
        )
    insert_executor(
        conn,
        signal_key="real",
        symbol="ETHUSDT",
        action="WATCH",
        reason="entry_blocked_volume_impulse",
        volume_impulse=1.18,
        required_volume_impulse=1.2,
        diagnostics_json={
            "volume_impulse_source": "meta.volume_spike",
            "volume_impulse_missing": False,
            "volume_impulse_ratio_to_required": 1.18 / 1.2,
        },
    )
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports", min_sample=1)

    blockers = read_csv(tmp_path / "reports" / "learning_executor_blockers.csv")
    assert blockers[0]["volume_impulse_source_distribution"] == "meta.volume_spike:1;missing_default:4"
    assert blockers[0]["missing_default_volume_impulse_count"] == "4"
    assert blockers[0]["missing_default_volume_impulse_share"] == "0.8"
    assert blockers[0]["avg_volume_impulse_ratio_to_required"] != "0"
    assert "fix snapshot mapping" in blockers[0]["recommendation"]

    recommendations = read_csv(tmp_path / "reports" / "learning_recommendations.csv")
    assert any("fix snapshot mapping" in row["reason"] for row in recommendations)

def test_executor_blocker_report_recommends_volume_threshold_sensitivity_when_close(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_executor_table(conn)
    insert_executor(
        conn,
        signal_key="close",
        symbol="BTCUSDT",
        action="WATCH",
        reason="entry_blocked_volume_impulse",
        volume_impulse=1.05,
        required_volume_impulse=1.2,
    )
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports")

    rows = read_csv(tmp_path / "reports" / "learning_executor_blockers.csv")
    assert rows[0]["avg_volume_impulse"] == "1.05"
    assert rows[0]["avg_required_volume_impulse"] == "1.2"
    assert "threshold sensitivity" in rows[0]["recommendation"]


def test_executor_blocker_report_recommends_snapshot_review_when_volume_far_below(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_executor_table(conn)
    insert_executor(
        conn,
        signal_key="far",
        symbol="ETHUSDT",
        action="WATCH",
        reason="entry_blocked_volume_impulse",
        volume_impulse=0.4,
        required_volume_impulse=1.2,
    )
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports")

    rows = read_csv(tmp_path / "reports" / "learning_executor_blockers.csv")
    assert "snapshot mapping" in rows[0]["recommendation"]
    assert "far below required" in rows[0]["recommendation"]


def test_executor_trades_report_and_summary_are_generated(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_executor_trades_table(conn)
    insert_executor_trade(
        conn,
        trade_key="t1",
        signal_key="s1",
        symbol="BTCUSDT",
        r_result=1.0,
        max_gain_r=1.4,
        max_drawdown_r=-0.2,
        moved_to_breakeven=1,
    )
    insert_executor_trade(
        conn,
        trade_key="t2",
        signal_key="s2",
        symbol="BTCUSDT",
        r_result=-0.5,
        max_gain_r=0.2,
        max_drawdown_r=-0.7,
        exit_reason="exit_stop_loss_hit",
    )
    conn.commit()
    conn.close()

    summary = generate_learning_report(db_path, tmp_path / "reports", min_sample=2)

    rows = read_csv(tmp_path / "reports" / "learning_executor_trades.csv")
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["total_trades"] == "2"
    assert rows[0]["wins"] == "1"
    assert rows[0]["losses"] == "1"
    assert rows[0]["breakeven_moves"] == "1"
    assert rows[0]["total_r_result"] == "0.5"
    assert summary["total_executor_trades"] == 2
    assert summary["executor_trades_total_r"] == 0.5
    assert summary["executor_trades_avg_r"] == 0.25
    assert summary["executor_trade_exit_reason_counts"] == {
        "exit_sell_flow_dominance": 1,
        "exit_stop_loss_hit": 1,
    }


def test_recommendations_are_generated_for_high_sl_symbol(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_diagnoses_table(conn)
    for idx in range(5):
        insert_diagnosis(conn, signal_key=f"sl-{idx}", symbol="SOLUSDT", timeframe="5", outcome="SL", r_result=-1.0)
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports", min_sample=5)

    rows = read_csv(tmp_path / "reports" / "learning_recommendations.csv")
    assert any(row["scope"] == "SYMBOL" and row["symbol"] == "SOLUSDT" and row["suggested_direction"] == "review" for row in rows)


def test_recommendations_are_generated_for_high_tp_but_low_avg_r_symbol(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    conn = sqlite3.connect(db_path)
    create_diagnoses_table(conn)
    for idx in range(7):
        insert_diagnosis(conn, signal_key=f"tp-{idx}", symbol="XRPUSDT", timeframe="1", outcome="TP1", r_result=0.1)
    for idx in range(3):
        insert_diagnosis(conn, signal_key=f"amb-{idx}", symbol="XRPUSDT", timeframe="1", outcome="AMBIGUOUS", r_result=0.0)
    conn.commit()
    conn.close()

    generate_learning_report(db_path, tmp_path / "reports", min_sample=5)

    rows = read_csv(tmp_path / "reports" / "learning_recommendations.csv")
    assert any(row["symbol"] == "XRPUSDT" and row["parameter"] == "trailing / exit" for row in rows)


def test_json_summary_includes_required_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    sqlite3.connect(db_path).close()

    generate_learning_report(db_path, tmp_path / "reports", since_hours=12)

    summary = json.loads((tmp_path / "reports" / "learning_summary.json").read_text(encoding="utf-8"))
    assert set(summary) == {
        "generated_at",
        "since_hours",
        "total_signals",
        "total_diagnoses",
        "total_executor_decisions",
        "total_lifecycle_events",
        "total_executor_trades",
        "executor_trades_total_r",
        "executor_trades_avg_r",
        "executor_trade_exit_reason_counts",
        "outcome_counts",
        "executor_action_counts",
        "top_tp_symbols",
        "top_sl_symbols",
        "top_executor_blockers",
        "high_level_notes",
    }
    assert summary["since_hours"] == 12


def test_csv_files_have_stable_headers(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    sqlite3.connect(db_path).close()

    generate_learning_report(db_path, tmp_path / "reports")

    expected = {
        "learning_symbol_edge.csv": SYMBOL_EDGE_HEADERS,
        "learning_timeframe_edge.csv": TIMEFRAME_EDGE_HEADERS,
        "learning_executor_blockers.csv": EXECUTOR_BLOCKERS_HEADERS,
        "learning_executor_trades.csv": EXECUTOR_TRADES_HEADERS,
        "learning_diagnosis_summary.csv": DIAGNOSIS_SUMMARY_HEADERS,
        "learning_recommendations.csv": RECOMMENDATIONS_HEADERS,
    }
    for filename, headers in expected.items():
        with (tmp_path / "reports" / filename).open(newline="", encoding="utf-8") as handle:
            assert next(csv.reader(handle)) == headers


def test_cli_main_can_run_against_temp_db(tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    sqlite3.connect(db_path).close()
    out_dir = tmp_path / "reports"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.learning_report",
            "--db",
            str(db_path),
            "--out-dir",
            str(out_dir),
            "--since-hours",
            "24",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert (out_dir / "learning_summary.json").exists()
