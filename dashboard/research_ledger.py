from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


_EPSILON = 1e-12

_OPEN_STATES = (
    "ENTERED",
    "PROTECT_BREAKEVEN",
    "TRAILING_PROFIT",
)


def _round4(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 4)


def _float_or_none(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: object | None) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _json_object(value: object | None) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)

    if value in (None, ""):
        return {}

    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}

    return dict(parsed) if isinstance(parsed, dict) else {}


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _public_run(row: sqlite3.Row) -> dict[str, object]:
    return {
        "run_id": str(row["run_id"]),
        "strategy_id": str(row["strategy_id"]),
        "strategy_version": str(row["strategy_version"]),
        "mode": str(row["mode"]),
        "code_sha": str(row["code_sha"]),
        "config_hash": str(row["config_hash"]),
        "label": (
            str(row["label"])
            if row["label"] not in (None, "")
            else None
        ),
        "status": str(row["status"]),
        "started_at": str(row["started_at"]),
        "last_seen_at": str(row["last_seen_at"]),
        "ended_at": (
            str(row["ended_at"])
            if row["ended_at"] not in (None, "")
            else None
        ),
    }


def _empty_summary() -> dict[str, object]:
    return {
        "total_open_trades": 0,
        "total_closed_trades": 0,
        "r_evaluated_trades": 0,
        "unscored_closed_trades": 0,
        "wins": 0,
        "losses": 0,
        "breakeven": 0,
        "win_rate": None,
        "net_r": 0.0,
        "avg_r": None,
        "profit_factor": None,
    }


def _empty_payload() -> dict[str, object]:
    return {
        "scope": "research",
        "available": False,
        "legacy_excluded": True,
        "run": None,
        "active_run": None,
        "runs": [],
        "summary": _empty_summary(),
        "open_trades": [],
        "closed_trades": [],
        "exit_reasons": [],
    }


def list_research_runs(
    db_path: str | Path,
    limit: int = 100,
) -> list[dict[str, object]]:
    path = Path(db_path)

    if not path.exists():
        return []

    safe_limit = max(1, min(int(limit or 100), 1000))
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row

    try:
        if not _has_table(conn, "research_runs"):
            return []

        rows = conn.execute(
            """
            SELECT
                run_id,
                strategy_id,
                strategy_version,
                mode,
                code_sha,
                config_hash,
                label,
                status,
                started_at,
                last_seen_at,
                ended_at
            FROM research_runs
            ORDER BY
                CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END,
                started_at DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        return [_public_run(row) for row in rows]
    finally:
        conn.close()


def _selected_run(
    runs: list[dict[str, object]],
    run_id: str | None,
) -> dict[str, object] | None:
    requested = str(run_id or "").strip()

    if requested:
        return next(
            (
                run
                for run in runs
                if str(run["run_id"]) == requested
            ),
            None,
        )

    return runs[0] if runs else None


def _read_open_trades(
    conn: sqlite3.Connection,
    run_id: str,
    limit: int,
) -> list[dict[str, object]]:
    placeholders = ", ".join("?" for _ in _OPEN_STATES)

    rows = conn.execute(
        f"""
        SELECT
            signal_key,
            symbol,
            timeframe,
            side,
            state,
            action,
            reason,
            entry_price,
            current_sl,
            max_gain_r,
            max_drawdown_r,
            bars_in_trade,
            signal_kind,
            btc_regime,
            market_regime,
            diagnostics_json,
            first_seen_at,
            last_seen_at,
            observation_count
        FROM research_executor_observations
        WHERE run_id = ?
          AND UPPER(COALESCE(state, '')) IN ({placeholders})
        ORDER BY last_seen_at DESC
        LIMIT ?
        """,
        (run_id, *_OPEN_STATES, limit),
    ).fetchall()

    result: list[dict[str, object]] = []

    for row in rows:
        diagnostics = _json_object(row["diagnostics_json"])
        state = str(row["state"] or "")
        breakeven_active = state in {
            "PROTECT_BREAKEVEN",
            "TRAILING_PROFIT",
        }

        result.append(
            {
                "signal_key": str(row["signal_key"]),
                "symbol": str(row["symbol"]),
                "timeframe": (
                    str(row["timeframe"])
                    if row["timeframe"] not in (None, "")
                    else None
                ),
                "side": (
                    str(row["side"])
                    if row["side"] not in (None, "")
                    else None
                ),
                "state": state or None,
                "action": (
                    str(row["action"])
                    if row["action"] not in (None, "")
                    else None
                ),
                "reason": (
                    str(row["reason"])
                    if row["reason"] not in (None, "")
                    else None
                ),
                "entry_price": _round4(
                    _float_or_none(row["entry_price"])
                ),
                "current_sl": _round4(
                    _float_or_none(row["current_sl"])
                ),
                "max_gain_r": _round4(
                    _float_or_none(row["max_gain_r"])
                ),
                "max_drawdown_r": _round4(
                    _float_or_none(row["max_drawdown_r"])
                ),
                "bars_in_trade": _int_or_zero(
                    row["bars_in_trade"]
                ),
                "signal_kind": (
                    str(row["signal_kind"])
                    if row["signal_kind"] not in (None, "")
                    else None
                ),
                "btc_regime": (
                    str(row["btc_regime"])
                    if row["btc_regime"] not in (None, "")
                    else None
                ),
                "market_regime": (
                    str(row["market_regime"])
                    if row["market_regime"] not in (None, "")
                    else None
                ),
                "breakeven_active": breakeven_active,
                "breakeven_display_time": (
                    diagnostics.get("breakeven_time")
                    if breakeven_active
                    else None
                ),
                "first_seen_at": str(row["first_seen_at"]),
                "updated_at": str(row["last_seen_at"]),
                "observation_count": _int_or_zero(
                    row["observation_count"]
                ),
            }
        )

    return result


def _read_closed_trades(
    conn: sqlite3.Connection,
    run_id: str,
    limit: int,
) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT
            trade_key,
            signal_key,
            symbol,
            timeframe,
            side,
            state,
            entry_price,
            exit_price,
            initial_sl,
            final_sl,
            exit_reason,
            r_result,
            max_gain_r,
            max_drawdown_r,
            bars_in_trade,
            duration_minutes,
            moved_to_breakeven,
            entry_time,
            exit_time,
            signal_kind,
            btc_regime,
            market_regime,
            diagnostics_json,
            first_recorded_at,
            updated_at
        FROM research_executor_trades
        WHERE run_id = ?
        ORDER BY exit_time DESC, updated_at DESC
        LIMIT ?
        """,
        (run_id, limit),
    ).fetchall()

    result: list[dict[str, object]] = []

    for row in rows:
        diagnostics = _json_object(row["diagnostics_json"])

        result.append(
            {
                "trade_key": str(row["trade_key"]),
                "signal_key": str(row["signal_key"]),
                "symbol": str(row["symbol"]),
                "timeframe": (
                    str(row["timeframe"])
                    if row["timeframe"] not in (None, "")
                    else None
                ),
                "side": (
                    str(row["side"])
                    if row["side"] not in (None, "")
                    else None
                ),
                "state": (
                    str(row["state"])
                    if row["state"] not in (None, "")
                    else None
                ),
                "entry_price": _round4(
                    _float_or_none(row["entry_price"])
                ),
                "exit_price": _round4(
                    _float_or_none(row["exit_price"])
                ),
                "initial_sl": _round4(
                    _float_or_none(row["initial_sl"])
                ),
                "final_sl": _round4(
                    _float_or_none(row["final_sl"])
                ),
                "current_sl": _round4(
                    _float_or_none(row["final_sl"])
                ),
                "exit_reason": (
                    str(row["exit_reason"])
                    if row["exit_reason"] not in (None, "")
                    else None
                ),
                "r_result": _round4(
                    _float_or_none(row["r_result"])
                ),
                "max_gain_r": _round4(
                    _float_or_none(row["max_gain_r"])
                ),
                "max_drawdown_r": _round4(
                    _float_or_none(row["max_drawdown_r"])
                ),
                "bars_in_trade": _int_or_zero(
                    row["bars_in_trade"]
                ),
                "duration_minutes": _round4(
                    _float_or_none(row["duration_minutes"])
                ),
                "moved_to_breakeven": bool(
                    _int_or_zero(row["moved_to_breakeven"])
                ),
                "breakeven_time": diagnostics.get(
                    "breakeven_time"
                ),
                "entry_time": (
                    str(row["entry_time"])
                    if row["entry_time"] not in (None, "")
                    else None
                ),
                "exit_time": (
                    str(row["exit_time"])
                    if row["exit_time"] not in (None, "")
                    else None
                ),
                "signal_kind": (
                    str(row["signal_kind"])
                    if row["signal_kind"] not in (None, "")
                    else None
                ),
                "btc_regime": (
                    str(row["btc_regime"])
                    if row["btc_regime"] not in (None, "")
                    else None
                ),
                "market_regime": (
                    str(row["market_regime"])
                    if row["market_regime"] not in (None, "")
                    else None
                ),
                "first_recorded_at": str(
                    row["first_recorded_at"]
                ),
                "updated_at": str(row["updated_at"]),
            }
        )

    return result


def _summarize_closed_trades(
    closed_trades: list[dict[str, object]],
) -> dict[str, object]:
    summary = _empty_summary()
    summary["total_closed_trades"] = len(closed_trades)

    r_values = [
        float(trade["r_result"])
        for trade in closed_trades
        if trade["r_result"] is not None
    ]

    summary["r_evaluated_trades"] = len(r_values)
    summary["unscored_closed_trades"] = (
        len(closed_trades) - len(r_values)
    )

    wins = sum(value > _EPSILON for value in r_values)
    losses = sum(value < -_EPSILON for value in r_values)
    breakeven = len(r_values) - wins - losses

    summary["wins"] = wins
    summary["losses"] = losses
    summary["breakeven"] = breakeven

    if not r_values:
        return summary

    net_r = sum(r_values)
    positive_r = sum(value for value in r_values if value > 0)
    negative_r = sum(value for value in r_values if value < 0)

    summary["win_rate"] = _round4(wins / len(r_values))
    summary["net_r"] = _round4(net_r) or 0.0
    summary["avg_r"] = _round4(net_r / len(r_values))

    if negative_r < -_EPSILON:
        summary["profit_factor"] = _round4(
            positive_r / abs(negative_r)
        )

    return summary


def _exit_reason_breakdown(
    closed_trades: list[dict[str, object]],
) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}

    for trade in closed_trades:
        reason = str(trade["exit_reason"] or "UNKNOWN")

        bucket = grouped.setdefault(
            reason,
            {
                "exit_reason": reason,
                "total": 0,
                "r_evaluated_trades": 0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "net_r": 0.0,
                "avg_r": None,
            },
        )

        bucket["total"] = int(bucket["total"]) + 1

        result = trade["r_result"]
        if result is None:
            continue

        r_value = float(result)
        bucket["r_evaluated_trades"] = (
            int(bucket["r_evaluated_trades"]) + 1
        )
        bucket["net_r"] = float(bucket["net_r"]) + r_value

        if r_value > _EPSILON:
            bucket["wins"] = int(bucket["wins"]) + 1
        elif r_value < -_EPSILON:
            bucket["losses"] = int(bucket["losses"]) + 1
        else:
            bucket["breakeven"] = int(bucket["breakeven"]) + 1

    rows: list[dict[str, object]] = []

    for bucket in grouped.values():
        evaluated = int(bucket["r_evaluated_trades"])
        net_r = _round4(float(bucket["net_r"])) or 0.0

        bucket["net_r"] = net_r
        bucket["avg_r"] = (
            _round4(net_r / evaluated)
            if evaluated > 0
            else None
        )

        rows.append(bucket)

    return sorted(
        rows,
        key=lambda row: (
            -int(row["total"]),
            str(row["exit_reason"]),
        ),
    )


def read_research_ledger(
    db_path: str | Path,
    *,
    run_id: str | None = None,
    limit: int = 50,
) -> dict[str, object]:
    path = Path(db_path)
    payload = _empty_payload()

    if not path.exists():
        return payload

    safe_limit = max(1, min(int(limit or 50), 2000))
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row

    try:
        required_tables = {
            "research_runs",
            "research_executor_observations",
            "research_executor_trades",
        }

        if not all(
            _has_table(conn, table_name)
            for table_name in required_tables
        ):
            return payload

        runs = list_research_runs(path, limit=1000)
        selected = _selected_run(runs, run_id)

        payload["available"] = bool(runs)
        payload["runs"] = runs
        payload["run"] = selected
        payload["active_run"] = selected

        if selected is None:
            return payload

        selected_run_id = str(selected["run_id"])

        open_trades = _read_open_trades(
            conn,
            selected_run_id,
            safe_limit,
        )
        closed_trades = _read_closed_trades(
            conn,
            selected_run_id,
            safe_limit,
        )

        summary = _summarize_closed_trades(closed_trades)
        summary["total_open_trades"] = len(open_trades)

        payload["summary"] = summary
        payload["open_trades"] = open_trades
        payload["closed_trades"] = closed_trades
        payload["exit_reasons"] = _exit_reason_breakdown(
            closed_trades
        )

        return payload
    finally:
        conn.close()
