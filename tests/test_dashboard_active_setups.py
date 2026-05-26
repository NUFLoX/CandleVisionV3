from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

import dashboard.server as server_module


def _init_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
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
            reasons_first,reasons_last,meta,first_seen,last_seen,repeat_count,status,max_gain_pct,max_drawdown_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "BTCUSDT|linear|15|PRE_IMPULSE_ZONE|Buy", "BTCUSDT", "linear", "15", "orderflow", "PRE_IMPULSE_ZONE", "Buy",
            8.0, 9.5, 10.0, 100.0, 95.0, 110.0, 120.0,
            "[]", "[]", "{}", "2026-05-01T00:00:00+00:00", "2026-05-02T00:00:00+00:00", 3, "PRE_IMPULSE", 3.2, -1.1,
        ),
    )
    conn.commit()
    conn.close()


def test_active_setups_endpoint_reads_from_signals_db(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(db_path)
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)

    app = server_module.create_app()
    with TestClient(app) as client:
        response = client.get("/api/active-setups?limit=10")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["status"] == "PRE_IMPULSE"
    assert rows[0]["repeat_count"] == 3
