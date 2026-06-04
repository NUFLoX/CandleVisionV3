from __future__ import annotations

import argparse
import csv
import importlib
import json
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

THRESHOLDS = (10, 20, 50)
DEFAULT_DB_PATH = Path("data/signals.db")
DEFAULT_OUT_DIR = Path("reports_profit_backtest")
DEFAULT_STAKE_USD = 10.0

RAW_REPORT = "signal_profit_backtest.csv"
BY_KIND_REPORT = "signal_profit_by_kind.csv"
SUMMARY_REPORT = "signal_profit_summary.json"

SOURCE_COLUMNS = (
    "signal_key",
    "symbol",
    "market",
    "timeframe",
    "source",
    "kind",
    "side",
    "entry",
    "stop_loss",
    "take_profit_1",
    "take_profit_2",
    "score_first",
    "score_last",
    "score_max",
    "status",
    "outcome",
    "first_seen",
    "last_seen",
    "repeat_count",
    "max_gain_pct",
    "max_drawdown_pct",
)

RAW_COLUMNS = (
    *SOURCE_COLUMNS,
    "potential_profit_usd",
    "hit_10_pct",
    "hit_20_pct",
    "hit_50_pct",
    "first_touch_win",
    "first_touch_profit_usd",
)

BY_KIND_COLUMNS = (
    "kind",
    "total",
    "valid_signals",
    "avg_max_gain_pct",
    "median_max_gain_pct",
    "max_gain_pct",
    "total_potential_profit_usd",
    "avg_potential_profit_usd",
    "hit_10_pct_share",
    "hit_20_pct_share",
    "hit_50_pct_share",
    "first_touch_total_profit_usd",
    "first_touch_avg_profit_usd",
    "first_touch_win_rate",
)

WIN_OUTCOME_TOKENS = {"TP", "TP1", "TP2", "TAKE_PROFIT", "TAKE_PROFIT_1", "TAKE_PROFIT_2", "WIN", "WON"}
LOSS_OUTCOME_TOKENS = {"SL", "STOP", "STOP_LOSS", "LOSS", "LOST"}


def _normalize_kind(kind: object) -> str:
    module_name = "orderflow_accum.signal_taxonomy"
    try:
        taxonomy = importlib.import_module(module_name)
        normalizer = getattr(taxonomy, "normalize_signal_kind")
        return str(normalizer(kind) or "").strip().upper()
    except (ImportError, AttributeError, TypeError, ValueError):
        return str(kind or "").strip().upper()


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).strip()))
    except ValueError:
        return None


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 4)


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return value


def _side_is_short(side: object) -> bool:
    return str(side or "").strip().upper() in {"SELL", "SHORT", "BEAR", "BEARISH"}


def _pct_from_entry(entry: float | None, target: float | None, side: object) -> float | None:
    if entry is None or target is None or entry == 0:
        return None
    if _side_is_short(side):
        return (entry - target) / entry * 100.0
    return (target - entry) / entry * 100.0


def _outcome_token(outcome: object) -> str:
    return str(outcome or "").strip().upper().replace("-", "_").replace(" ", "_")


def _first_touch_win(outcome: object) -> int | None:
    token = _outcome_token(outcome)
    if not token:
        return None
    if token in WIN_OUTCOME_TOKENS or token.startswith("TP") or "TAKE_PROFIT" in token:
        return 1
    if token in LOSS_OUTCOME_TOKENS or token.startswith("SL") or "STOP_LOSS" in token:
        return 0
    return None


def _first_touch_profit_usd(row: dict[str, Any], stake_usd: float, potential_profit_usd: float | None) -> float | None:
    win = row["first_touch_win"]
    entry = _to_float(row.get("entry"))
    side = row.get("side")
    if win == 1:
        tp_pct = _pct_from_entry(entry, _to_float(row.get("take_profit_1")), side)
        if tp_pct is not None:
            return stake_usd * tp_pct / 100.0
        return potential_profit_usd
    if win == 0:
        sl_pct = _pct_from_entry(entry, _to_float(row.get("stop_loss")), side)
        if sl_pct is not None:
            return stake_usd * sl_pct / 100.0
        drawdown = _to_float(row.get("max_drawdown_pct"))
        if drawdown is not None:
            return stake_usd * drawdown / 100.0
    return None


def _empty_source_row() -> dict[str, Any]:
    return {column: None for column in SOURCE_COLUMNS}


def _read_signal_rows(db_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    notes: list[str] = []
    if not db_path.exists():
        return [], ["signals database is missing"]

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        return [], [f"signals database could not be opened: {exc}"]

    try:
        table_row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='signals'"
        ).fetchone()
        if table_row is None:
            return [], ["signals table is missing"]

        columns = {row[1] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        selected = [column for column in SOURCE_COLUMNS if column in columns]
        if not selected:
            return [], ["signals table has no supported columns"]

        quoted = ", ".join(f'"{column}"' for column in selected)
        rows = []
        for source in conn.execute(f"SELECT {quoted} FROM signals"):
            item = _empty_source_row()
            for column in selected:
                item[column] = source[column]
            rows.append(item)
        return rows, notes
    except sqlite3.Error as exc:
        return [], [f"signals table could not be read: {exc}"]
    finally:
        conn.close()


def _build_raw_rows(source_rows: list[dict[str, Any]], stake_usd: float) -> list[dict[str, Any]]:
    raw_rows: list[dict[str, Any]] = []
    for source in source_rows:
        row = dict(source)
        row["kind"] = _normalize_kind(row.get("kind"))
        max_gain = _to_float(row.get("max_gain_pct"))
        potential = stake_usd * max_gain / 100.0 if max_gain is not None else None
        row["potential_profit_usd"] = _round(potential)
        for threshold in THRESHOLDS:
            row[f"hit_{threshold}_pct"] = None if max_gain is None else int(max_gain >= threshold)
        row["first_touch_win"] = _first_touch_win(row.get("outcome"))
        row["first_touch_profit_usd"] = _round(_first_touch_profit_usd(row, stake_usd, potential))
        row["max_gain_pct"] = _round(max_gain)
        row["max_drawdown_pct"] = _round(_to_float(row.get("max_drawdown_pct")))
        numeric_columns = (
            "entry",
            "stop_loss",
            "take_profit_1",
            "take_profit_2",
            "score_first",
            "score_last",
            "score_max",
        )
        for column in numeric_columns:
            row[column] = _round(_to_float(row.get(column)))
        row["repeat_count"] = _to_int(row.get("repeat_count"))
        raw_rows.append(row)
    return raw_rows


def _avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _share(count: int, total: int) -> float | None:
    if total <= 0:
        return None
    return count / total


def _aggregate_rows(raw_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in raw_rows:
        kind = str(row.get("kind") or "").strip().upper()
        if not kind:
            continue
        grouped.setdefault(kind, []).append(row)

    by_kind_rows: list[dict[str, Any]] = []
    for kind in sorted(grouped):
        items = grouped[kind]
        gains = [float(row["max_gain_pct"]) for row in items if row.get("max_gain_pct") is not None]
        potentials = [
            float(row["potential_profit_usd"])
            for row in items
            if row.get("potential_profit_usd") is not None
        ]
        first_touch_profits = [
            float(row["first_touch_profit_usd"])
            for row in items
            if row.get("first_touch_profit_usd") is not None
        ]
        first_touch_wins = [
            int(row["first_touch_win"])
            for row in items
            if row.get("first_touch_win") is not None
        ]
        valid = len(gains)
        by_kind_rows.append(
            {
                "kind": kind,
                "total": len(items),
                "valid_signals": valid,
                "avg_max_gain_pct": _round(_avg(gains)),
                "median_max_gain_pct": _round(float(statistics.median(gains)) if gains else None),
                "max_gain_pct": _round(max(gains) if gains else None),
                "total_potential_profit_usd": _round(sum(potentials) if potentials else None),
                "avg_potential_profit_usd": _round(_avg(potentials)),
                "hit_10_pct_share": _round(_share(sum(1 for value in gains if value >= 10.0), valid)),
                "hit_20_pct_share": _round(_share(sum(1 for value in gains if value >= 20.0), valid)),
                "hit_50_pct_share": _round(_share(sum(1 for value in gains if value >= 50.0), valid)),
                "first_touch_total_profit_usd": _round(sum(first_touch_profits) if first_touch_profits else None),
                "first_touch_avg_profit_usd": _round(_avg(first_touch_profits)),
                "first_touch_win_rate": _round(
                    _share(sum(1 for value in first_touch_wins if value == 1), len(first_touch_wins))
                ),
            }
        )

    overall_gains = [float(row["max_gain_pct"]) for row in raw_rows if row.get("max_gain_pct") is not None]
    overall_potentials = [
        float(row["potential_profit_usd"])
        for row in raw_rows
        if row.get("potential_profit_usd") is not None
    ]
    overall_first_touch_profits = [
        float(row["first_touch_profit_usd"])
        for row in raw_rows
        if row.get("first_touch_profit_usd") is not None
    ]
    overall_first_touch_wins = [
        int(row["first_touch_win"])
        for row in raw_rows
        if row.get("first_touch_win") is not None
    ]
    valid_total = len(overall_gains)
    overall = {
        "avg_max_gain_pct": _round(_avg(overall_gains)),
        "median_max_gain_pct": _round(float(statistics.median(overall_gains)) if overall_gains else None),
        "max_gain_pct": _round(max(overall_gains) if overall_gains else None),
        "hit_10_pct_share": _round(_share(sum(1 for value in overall_gains if value >= 10.0), valid_total)),
        "hit_20_pct_share": _round(_share(sum(1 for value in overall_gains if value >= 20.0), valid_total)),
        "hit_50_pct_share": _round(_share(sum(1 for value in overall_gains if value >= 50.0), valid_total)),
        "total_potential_profit_usd": _round(sum(overall_potentials) if overall_potentials else None),
        "avg_potential_profit_usd": _round(_avg(overall_potentials)),
        "first_touch_total_profit_usd": _round(
            sum(overall_first_touch_profits) if overall_first_touch_profits else None
        ),
        "first_touch_avg_profit_usd": _round(_avg(overall_first_touch_profits)),
        "first_touch_win_rate": _round(
            _share(
                sum(1 for value in overall_first_touch_wins if value == 1),
                len(overall_first_touch_wins),
            )
        ),
    }
    return by_kind_rows, overall


def _write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})


def generate_profit_backtest_report(
    db_path: str | Path = DEFAULT_DB_PATH,
    out_dir: str | Path = DEFAULT_OUT_DIR,
    stake_usd: float = DEFAULT_STAKE_USD,
) -> dict[str, Any]:
    db_path = Path(db_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    source_rows, notes = _read_signal_rows(db_path)
    raw_rows = _build_raw_rows(source_rows, float(stake_usd))
    by_kind_rows, overall = _aggregate_rows(raw_rows)

    _write_csv(out_dir / RAW_REPORT, RAW_COLUMNS, raw_rows)
    _write_csv(out_dir / BY_KIND_REPORT, BY_KIND_COLUMNS, by_kind_rows)

    summary: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "out_dir": str(out_dir),
        "stake_usd": float(stake_usd),
        "total_signals": len(raw_rows),
        "valid_signals": sum(1 for row in raw_rows if row.get("max_gain_pct") is not None),
        "kinds_count": len(by_kind_rows),
        "thresholds": list(THRESHOLDS),
        "reports": [BY_KIND_REPORT, RAW_REPORT],
        "notes": notes,
        **overall,
    }
    (out_dir / SUMMARY_REPORT).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate offline profit-backtest reports for Signal Intelligence.")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to signals SQLite database.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Directory for generated report files.")
    parser.add_argument(
        "--stake-usd",
        type=float,
        default=DEFAULT_STAKE_USD,
        help="Stake in USD used for profit approximations.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    summary = generate_profit_backtest_report(args.db, args.out_dir, args.stake_usd)
    print(
        "profit-backtest report generated: "
        f"total_signals={summary['total_signals']} valid_signals={summary['valid_signals']} "
        f"out_dir={summary['out_dir']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
