from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


REQUIRED_SIGNAL_COLUMNS = {
    "signal_key",
    "symbol",
    "market",
    "timeframe",
    "kind",
    "side",
    "score_first",
    "score_last",
    "score_max",
    "first_seen",
    "last_seen",
    "repeat_count",
    "status",
}

REQUIRED_EVENT_COLUMNS = {
    "signal_key",
    "symbol",
    "timeframe",
    "event_type",
    "from_status",
    "to_status",
    "created_at",
}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def run_check(db_path: Path) -> dict:
    out: dict[str, object] = {
        "db_exists": db_path.exists(),
        "signals_table": False,
        "signal_events_table": False,
        "signals_columns_ok": False,
        "signal_events_columns_ok": False,
        "missing_signals_columns": [],
        "missing_signal_events_columns": [],
        "status": "fail",
    }
    if not db_path.exists():
        return out

    conn = sqlite3.connect(str(db_path))
    try:
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        out["signals_table"] = "signals" in tables
        out["signal_events_table"] = "signal_events" in tables

        if out["signals_table"]:
            cols = _table_columns(conn, "signals")
            missing = sorted(REQUIRED_SIGNAL_COLUMNS - cols)
            out["missing_signals_columns"] = missing
            out["signals_columns_ok"] = len(missing) == 0

        if out["signal_events_table"]:
            cols = _table_columns(conn, "signal_events")
            missing = sorted(REQUIRED_EVENT_COLUMNS - cols)
            out["missing_signal_events_columns"] = missing
            out["signal_events_columns_ok"] = len(missing) == 0
    finally:
        conn.close()

    if (
        out["db_exists"]
        and out["signals_table"]
        and out["signal_events_table"]
        and out["signals_columns_ok"]
        and out["signal_events_columns_ok"]
    ):
        out["status"] = "ok"
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Final readiness check for signals.db schema")
    parser.add_argument("--db", default="data/signals.db")
    args = parser.parse_args()

    result = run_check(Path(args.db))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
