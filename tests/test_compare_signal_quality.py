from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def _mk_db(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT,
            reasons_last TEXT,
            timeframe TEXT,
            repeat_count INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO signals(status,reasons_last,timeframe,repeat_count) VALUES (?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_compare_signal_quality_outputs_delta(tmp_path: Path) -> None:
    before = tmp_path / "before.db"
    after = tmp_path / "after.db"
    _mk_db(before, [
        ("TP1", '["a"]', "15", 2),
        ("SL", '["b"]', "15", 1),
    ])
    _mk_db(after, [
        ("TP1", '["a"]', "15", 1),
        ("TP2", '["a"]', "5", 1),
        ("SL", '["b"]', "15", 1),
    ])

    out = subprocess.run(
        [sys.executable, "tools/compare_signal_quality.py", "--before", str(before), "--after", str(after)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    payload = json.loads(out)
    assert "before" in payload and "after" in payload and "delta" in payload
    assert payload["delta"]["signals_total"] == 1
