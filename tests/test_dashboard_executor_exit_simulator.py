from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import dashboard.server as server_module


def _disable_dashboard_live_refresh(monkeypatch) -> None:
    async def idle_refresh_loop(*_args, **_kwargs):
        while True:
            await asyncio.sleep(3600)

    monkeypatch.setattr(server_module, "_live_refresh_loop", idle_refresh_loop)


def _client_for_db(db_path: Path, monkeypatch) -> TestClient:
    _disable_dashboard_live_refresh(monkeypatch)
    monkeypatch.setattr(server_module, "SIGNALS_DB_PATH", db_path)
    return TestClient(server_module.create_app(), raise_server_exceptions=False)


def _init_db(path: Path, rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE executor_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL UNIQUE,
                signal_key TEXT,
                symbol TEXT,
                timeframe TEXT,
                side TEXT,
                state TEXT,
                exit_reason TEXT,
                r_result REAL,
                max_gain_r REAL,
                max_drawdown_r REAL,
                moved_to_breakeven INTEGER,
                exit_time TEXT,
                updated_at TEXT,
                diagnostics_json TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO executor_trades (
                trade_key, signal_key, symbol, timeframe, side, state, exit_reason, r_result,
                max_gain_r, max_drawdown_r, moved_to_breakeven, exit_time, updated_at, diagnostics_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _rule(payload: dict[str, object], rule_id: str) -> dict[str, object]:
    return {row["rule_id"]: row for row in payload["rules"]}[rule_id]  # type: ignore[index]


def test_missing_db_returns_safe_payload_and_rules(monkeypatch, tmp_path: Path) -> None:
    missing_db = tmp_path / "missing.db"
    with _client_for_db(missing_db, monkeypatch) as client:
        response = client.get("/api/executor-exit-simulator")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_trades"] == 0
    assert payload["summary"]["current_net_r"] == 0.0
    assert len(payload["rules"]) >= 8
    assert payload["by_kind"] == []
    assert payload["by_timeframe"] == []
    assert payload["trade_simulations"] == []


def test_missing_executor_trades_table_returns_safe_payload(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE signals (signal_key TEXT, kind TEXT, timeframe TEXT)")

    with _client_for_db(db_path, monkeypatch) as client:
        response = client.get("/api/executor-exit-simulator")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_trades"] == 0
    assert len(payload["rules"]) >= 8


def test_basic_simulation_rules(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(
        db_path,
        [
            ("trade1", "ENAUSDT|linear|5|PRE_IMPULSE_ZONE|Buy", "ENAUSDT", "5", "Buy", "EXITED", "STOP_LOSS", -1.0, 1.2, -1.0, 0, "2026-06-04T10:00:00+00:00", "2026-06-04T10:00:00+00:00", None),
            ("trade2", "XLMUSDT|linear|5|PRE_IMPULSE_ZONE|Buy", "XLMUSDT", "5", "Buy", "EXITED", "MANUAL", 0.1, 1.5, -0.2, 1, "2026-06-04T11:00:00+00:00", "2026-06-04T11:00:00+00:00", None),
            ("trade3", "ADAUSDT|linear|15|ABSORPTION_ZONE|Buy", "ADAUSDT", "15", "Buy", "EXITED", "STOP_LOSS", -1.0, 0.2, -1.0, 0, "2026-06-04T12:00:00+00:00", "2026-06-04T12:00:00+00:00", None),
        ],
    )

    with _client_for_db(db_path, monkeypatch) as client:
        response = client.get("/api/executor-exit-simulator")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["current_net_r"] == -1.9

    no_loss = _rule(payload, "no_full_loss_after_1r")
    assert no_loss["simulated_net_r"] == -0.9
    assert no_loss["delta_net_r_vs_actual"] == 1.0

    lock_quarter = _rule(payload, "lock_0_25r_after_1r")
    assert lock_quarter["simulated_net_r"] == -0.5
    assert lock_quarter["delta_net_r_vs_actual"] == 1.4
    assert lock_quarter["improved_trades"] == 2

    trade3 = next(row for row in payload["trade_simulations"] if row["symbol"] == "ADAUSDT")
    assert trade3["best_simulated_r_for_trade"] == -1.0
    assert trade3["best_delta_r_for_trade"] == 0.0

    assert no_loss["delta_net_r_vs_actual"] > 0
    assert lock_quarter["delta_net_r_vs_actual"] > 0


def test_trailing_40pct_giveback_after_1r(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(
        db_path,
        [
            ("trade1", "BTCUSDT|linear|5|PRE_IMPULSE_ZONE|Buy", "BTCUSDT", "5", "Buy", "EXITED", "STOP_LOSS", -1.0, 2.0, -1.0, 0, "2026-06-04T10:00:00+00:00", "2026-06-04T10:00:00+00:00", None),
        ],
    )

    with _client_for_db(db_path, monkeypatch) as client:
        payload = client.get("/api/executor-exit-simulator").json()

    trailing = _rule(payload, "trailing_40pct_giveback_after_1r")
    assert trailing["simulated_net_r"] == 1.2
    assert payload["trade_simulations"][0]["best_simulated_r_for_trade"] == 1.2


def test_grouping_by_kind_and_timeframe_works_from_signal_key(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(
        db_path,
        [
            ("trade1", "ENAUSDT|linear|5|PRE_IMPULSE_ZONE|Buy", "ENAUSDT", None, None, "EXITED", "STOP_LOSS", -1.0, 1.2, -1.0, 0, "2026-06-04T10:00:00+00:00", "2026-06-04T10:00:00+00:00", None),
            ("trade2", "XLMUSDT|linear|15|ABSORPTION_ZONE|Buy", "XLMUSDT", None, None, "EXITED", "STOP_LOSS", -1.0, 1.2, -1.0, 0, "2026-06-04T11:00:00+00:00", "2026-06-04T11:00:00+00:00", None),
        ],
    )

    with _client_for_db(db_path, monkeypatch) as client:
        payload = client.get("/api/executor-exit-simulator").json()

    by_kind = {row["kind"]: row for row in payload["by_kind"]}
    by_timeframe = {row["timeframe"]: row for row in payload["by_timeframe"]}
    assert by_kind["PRE_IMPULSE_ZONE"]["total_trades"] == 1
    assert by_kind["ABSORPTION_ZONE"]["total_trades"] == 1
    assert by_timeframe["5"]["total_trades"] == 1
    assert by_timeframe["15"]["total_trades"] == 1


def test_invalid_diagnostics_json_does_not_crash(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "signals.db"
    _init_db(
        db_path,
        [
            ("trade1", "BTCUSDT|linear|5|PRE_IMPULSE_ZONE|Buy", "BTCUSDT", "5", "Buy", "EXITED", "STOP_LOSS", -1.0, 1.0, -1.0, 0, "2026-06-04T10:00:00+00:00", "2026-06-04T10:00:00+00:00", "not-json"),
            ("trade2", "ETHUSDT|linear|5|ABSORPTION_ZONE|Buy", "ETHUSDT", "5", "Buy", "EXITED", "STOP_LOSS", -1.0, 1.0, -1.0, 0, "2026-06-04T11:00:00+00:00", "2026-06-04T11:00:00+00:00", json.dumps({"signal_kind": "ABSORPTION_ZONE"})),
        ],
    )

    with _client_for_db(db_path, monkeypatch) as client:
        response = client.get("/api/executor-exit-simulator")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_trades"] == 2
    assert {row["signal_kind"] for row in payload["trade_simulations"]} == {"PRE_IMPULSE_ZONE", "ABSORPTION_ZONE"}
