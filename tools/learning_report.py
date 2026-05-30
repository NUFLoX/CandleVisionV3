from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

SUMMARY_FILE = "learning_summary.json"
SYMBOL_EDGE_FILE = "learning_symbol_edge.csv"
TIMEFRAME_EDGE_FILE = "learning_timeframe_edge.csv"
EXECUTOR_BLOCKERS_FILE = "learning_executor_blockers.csv"
DIAGNOSIS_SUMMARY_FILE = "learning_diagnosis_summary.csv"
RECOMMENDATIONS_FILE = "learning_recommendations.csv"

SYMBOL_EDGE_HEADERS = [
    "symbol",
    "total_diagnoses",
    "tp_count",
    "sl_count",
    "expired_count",
    "ambiguous_count",
    "tp_rate",
    "sl_rate",
    "avg_r_result",
    "avg_max_gain_pct",
    "avg_max_drawdown_pct",
    "recommendation",
]
TIMEFRAME_EDGE_HEADERS = [
    "timeframe",
    "total_diagnoses",
    "tp_count",
    "sl_count",
    "expired_count",
    "ambiguous_count",
    "tp_rate",
    "sl_rate",
    "avg_r_result",
    "avg_max_gain_pct",
    "avg_max_drawdown_pct",
    "recommendation",
]
EXECUTOR_BLOCKERS_HEADERS = [
    "reason",
    "total",
    "symbols_count",
    "avg_max_gain_r",
    "avg_max_drawdown_r",
    "recommendation",
]
DIAGNOSIS_SUMMARY_HEADERS = [
    "outcome",
    "total",
    "avg_r_result",
    "avg_max_gain_pct",
    "avg_max_drawdown_pct",
    "common_recommendation",
]
RECOMMENDATIONS_HEADERS = [
    "scope",
    "symbol",
    "timeframe",
    "parameter",
    "current_value",
    "suggested_direction",
    "reason",
    "confidence",
    "sample_size",
    "status",
]

TP_OUTCOMES = {"TP", "TP1", "TP2", "TAKE_PROFIT", "TAKE_PROFIT_1", "TAKE_PROFIT_2"}
SL_OUTCOMES = {"SL", "STOP_LOSS"}
EXPIRED_OUTCOMES = {"EXPIRED", "TIMEOUT"}
AMBIGUOUS_OUTCOMES = {"AMBIGUOUS", "UNKNOWN"}
BLOCKER_REASONS = {
    "entry_blocked_volume_impulse": ("volume_impulse", "review volume_impulse threshold or snapshot mapping"),
    "entry_blocked_buy_flow": ("buy_flow/sell_flow ratio", "review buy_flow/sell_flow ratio"),
}


def utc_iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_avg(values: Iterable[Any]) -> float:
    numbers = [item for item in (safe_float(value) for value in values) if item is not None]
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def fmt_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".") if value else "0"


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def select_rows(conn: sqlite3.Connection, table: str, since: datetime | None) -> list[dict[str, Any]]:
    columns = table_columns(conn, table)
    if not columns:
        return []

    rows = [dict(row) for row in conn.execute(f"SELECT * FROM {table}").fetchall()]
    if since is None:
        return rows

    timestamp_column = next((col for col in ("created_at", "updated_at", "first_seen", "last_seen", "outcome_checked_at") if col in columns), None)
    if timestamp_column is None:
        return rows

    filtered: list[dict[str, Any]] = []
    for row in rows:
        parsed = parse_timestamp(row.get(timestamp_column))
        if parsed is None or parsed >= since:
            filtered.append(row)
    return filtered


def classify_outcome(value: Any) -> str:
    outcome = str(value or "").strip().upper()
    if outcome in TP_OUTCOMES or outcome.startswith("TP"):
        return "tp"
    if outcome in SL_OUTCOMES:
        return "sl"
    if outcome in EXPIRED_OUTCOMES:
        return "expired"
    if outcome in AMBIGUOUS_OUTCOMES:
        return "ambiguous"
    return "ambiguous"


def rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return count / total


def edge_recommendation(tp_rate: float, sl_rate: float, sample_size: int, min_sample: int, label: str) -> str:
    if sample_size < min_sample:
        return "collect more samples"
    if sl_rate >= 0.5:
        return f"review {label}: high SL rate"
    if tp_rate >= 0.7:
        return f"keep/watch {label}: strong TP rate"
    return "keep monitoring"


def build_edge_rows(rows: list[dict[str, Any]], group_col: str, headers: list[str], min_sample: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(group_col) or "UNKNOWN")].append(row)

    result: list[dict[str, Any]] = []
    for key, items in sorted(grouped.items()):
        total = len(items)
        counts = Counter(classify_outcome(item.get("outcome")) for item in items)
        tp_rate_value = rate(counts["tp"], total)
        sl_rate_value = rate(counts["sl"], total)
        output = {
            group_col: key,
            "total_diagnoses": str(total),
            "tp_count": str(counts["tp"]),
            "sl_count": str(counts["sl"]),
            "expired_count": str(counts["expired"]),
            "ambiguous_count": str(counts["ambiguous"]),
            "tp_rate": fmt_float(tp_rate_value),
            "sl_rate": fmt_float(sl_rate_value),
            "avg_r_result": fmt_float(safe_avg(item.get("r_result") for item in items)),
            "avg_max_gain_pct": fmt_float(safe_avg(item.get("max_gain_pct") for item in items)),
            "avg_max_drawdown_pct": fmt_float(safe_avg(item.get("max_drawdown_pct") for item in items)),
            "recommendation": edge_recommendation(tp_rate_value, sl_rate_value, total, min_sample, group_col),
        }
        result.append({header: output.get(header, "") for header in headers})
    return result


def build_executor_blocker_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        reason = str(row.get("reason") or "UNKNOWN")
        grouped[reason].append(row)

    result: list[dict[str, Any]] = []
    for reason, items in sorted(grouped.items(), key=lambda pair: (-len(pair[1]), pair[0])):
        recommendation = "review blocker frequency" if reason.startswith("entry_blocked") else "keep monitoring"
        if reason in BLOCKER_REASONS:
            recommendation = BLOCKER_REASONS[reason][1]
        result.append(
            {
                "reason": reason,
                "total": str(len(items)),
                "symbols_count": str(len({str(item.get("symbol") or "UNKNOWN") for item in items})),
                "avg_max_gain_r": fmt_float(safe_avg(item.get("max_gain_r") for item in items)),
                "avg_max_drawdown_r": fmt_float(safe_avg(item.get("max_drawdown_r") for item in items)),
                "recommendation": recommendation,
            }
        )
    return result


def build_diagnosis_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("outcome") or "UNKNOWN").upper()].append(row)

    result: list[dict[str, Any]] = []
    for outcome, items in sorted(grouped.items(), key=lambda pair: (-len(pair[1]), pair[0])):
        recommendations = Counter(str(item.get("recommendation") or "") for item in items if item.get("recommendation"))
        common_recommendation = recommendations.most_common(1)[0][0] if recommendations else ""
        result.append(
            {
                "outcome": outcome,
                "total": str(len(items)),
                "avg_r_result": fmt_float(safe_avg(item.get("r_result") for item in items)),
                "avg_max_gain_pct": fmt_float(safe_avg(item.get("max_gain_pct") for item in items)),
                "avg_max_drawdown_pct": fmt_float(safe_avg(item.get("max_drawdown_pct") for item in items)),
                "common_recommendation": common_recommendation,
            }
        )
    return result


def recommendation_confidence(sample_size: int, min_sample: int) -> str:
    if sample_size >= min_sample * 3:
        return "high"
    if sample_size >= min_sample:
        return "medium"
    return "low"


def recommendation_row(
    *,
    scope: str,
    symbol: str = "",
    timeframe: str = "",
    parameter: str,
    current_value: str,
    suggested_direction: str,
    reason: str,
    confidence: str,
    sample_size: int,
) -> dict[str, str]:
    return {
        "scope": scope,
        "symbol": symbol,
        "timeframe": timeframe,
        "parameter": parameter,
        "current_value": current_value,
        "suggested_direction": suggested_direction,
        "reason": reason,
        "confidence": confidence,
        "sample_size": str(sample_size),
        "status": "informational_only",
    }


def build_recommendation_rows(
    symbol_rows: list[dict[str, Any]],
    timeframe_rows: list[dict[str, Any]],
    executor_rows: list[dict[str, Any]],
    min_sample: int,
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []

    for row in symbol_rows:
        sample_size = int(row["total_diagnoses"] or 0)
        tp_rate_value = float(row["tp_rate"] or 0)
        sl_rate_value = float(row["sl_rate"] or 0)
        avg_r = float(row["avg_r_result"] or 0)
        if sample_size >= min_sample and sl_rate_value >= 0.5:
            result.append(
                recommendation_row(
                    scope="SYMBOL",
                    symbol=str(row["symbol"]),
                    parameter="entry timing / confirmation",
                    current_value=f"sl_rate={fmt_float(sl_rate_value)}",
                    suggested_direction="review",
                    reason="SL rate is at least 50%; review entry timing or require stricter confirmation.",
                    confidence=recommendation_confidence(sample_size, min_sample),
                    sample_size=sample_size,
                )
            )
        if sample_size >= min_sample and tp_rate_value >= 0.7 and avg_r <= 0.2:
            result.append(
                recommendation_row(
                    scope="SYMBOL",
                    symbol=str(row["symbol"]),
                    parameter="trailing / exit",
                    current_value=f"tp_rate={fmt_float(tp_rate_value)} avg_r_result={fmt_float(avg_r)}",
                    suggested_direction="review",
                    reason="TP rate is strong but average R is low; review trailing or exit capture.",
                    confidence=recommendation_confidence(sample_size, min_sample),
                    sample_size=sample_size,
                )
            )

    for row in timeframe_rows:
        sample_size = int(row["total_diagnoses"] or 0)
        tp_rate_value = float(row["tp_rate"] or 0)
        sl_rate_value = float(row["sl_rate"] or 0)
        if sample_size >= min_sample and tp_rate_value >= 0.7:
            result.append(
                recommendation_row(
                    scope="TIMEFRAME",
                    timeframe=str(row["timeframe"]),
                    parameter="timeframe edge",
                    current_value=f"tp_rate={fmt_float(tp_rate_value)}",
                    suggested_direction="keep",
                    reason="Strong timeframe: TP rate is at least 70%.",
                    confidence=recommendation_confidence(sample_size, min_sample),
                    sample_size=sample_size,
                )
            )
        if sample_size >= min_sample and sl_rate_value >= 0.5:
            result.append(
                recommendation_row(
                    scope="TIMEFRAME",
                    timeframe=str(row["timeframe"]),
                    parameter="timeframe noise",
                    current_value=f"sl_rate={fmt_float(sl_rate_value)}",
                    suggested_direction="review",
                    reason="Weak/noisy timeframe: SL rate is at least 50%.",
                    confidence=recommendation_confidence(sample_size, min_sample),
                    sample_size=sample_size,
                )
            )

    total_executor = sum(int(row["total"] or 0) for row in executor_rows)
    for row in executor_rows:
        reason = str(row["reason"])
        sample_size = int(row["total"] or 0)
        if reason in BLOCKER_REASONS and total_executor > 0 and sample_size / total_executor >= 0.5:
            parameter, recommendation = BLOCKER_REASONS[reason]
            result.append(
                recommendation_row(
                    scope="EXECUTOR",
                    parameter=parameter,
                    current_value=f"{reason} share={fmt_float(sample_size / total_executor)}",
                    suggested_direction="review",
                    reason=f"Executor blocker dominates recent decisions; {recommendation}.",
                    confidence=recommendation_confidence(sample_size, max(min_sample, 1)),
                    sample_size=sample_size,
                )
            )

    return sorted(result, key=lambda item: (item["scope"], item["symbol"], item["timeframe"], item["parameter"]))


def write_csv(path: Path, headers: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({header: row.get(header, "") for header in headers})


def build_summary(
    *,
    since_hours: int,
    signals: list[dict[str, Any]],
    diagnoses: list[dict[str, Any]],
    executor_outcomes: list[dict[str, Any]],
    lifecycle_events: list[dict[str, Any]],
    executor_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    outcome_counts = Counter(str(row.get("outcome") or "UNKNOWN").upper() for row in diagnoses)
    executor_action_counts = Counter(str(row.get("action") or "UNKNOWN") for row in executor_outcomes)
    symbol_outcomes: dict[str, Counter[str]] = defaultdict(Counter)
    for row in diagnoses:
        symbol_outcomes[str(row.get("symbol") or "UNKNOWN")][classify_outcome(row.get("outcome"))] += 1

    top_tp_symbols = [symbol for symbol, counts in sorted(symbol_outcomes.items(), key=lambda item: (-item[1]["tp"], item[0])) if counts["tp"] > 0][:10]
    top_sl_symbols = [symbol for symbol, counts in sorted(symbol_outcomes.items(), key=lambda item: (-item[1]["sl"], item[0])) if counts["sl"] > 0][:10]
    top_executor_blockers = [row["reason"] for row in executor_rows[:10]]
    high_level_notes = [
        "Learning report is informational only; no runtime configuration or trading behavior was changed.",
        f"Analyzed rows from the last {since_hours} hours when timestamp columns were available.",
    ]
    if not diagnoses:
        high_level_notes.append("No diagnoses found for this period.")
    if not executor_outcomes:
        high_level_notes.append("No executor decisions found for this period.")

    return {
        "generated_at": utc_iso_now(),
        "since_hours": since_hours,
        "total_signals": len(signals),
        "total_diagnoses": len(diagnoses),
        "total_executor_decisions": len(executor_outcomes),
        "total_lifecycle_events": len(lifecycle_events),
        "outcome_counts": dict(sorted(outcome_counts.items())),
        "executor_action_counts": dict(sorted(executor_action_counts.items())),
        "top_tp_symbols": top_tp_symbols,
        "top_sl_symbols": top_sl_symbols,
        "top_executor_blockers": top_executor_blockers,
        "high_level_notes": high_level_notes,
    }


def generate_learning_report(db_path: str | Path, out_dir: str | Path, since_hours: int = 24, min_sample: int = 5) -> dict[str, Any]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    db = Path(db_path)
    since = datetime.now(UTC) - timedelta(hours=since_hours) if since_hours > 0 else None

    if db.exists():
        conn = sqlite3.connect(str(db))
        conn.row_factory = sqlite3.Row
        try:
            signals = select_rows(conn, "signals", since)
            diagnoses = select_rows(conn, "trade_diagnoses", since)
            executor_outcomes = select_rows(conn, "executor_outcomes", since)
            lifecycle_events = select_rows(conn, "trade_lifecycle_events", since)
        finally:
            conn.close()
    else:
        signals = []
        diagnoses = []
        executor_outcomes = []
        lifecycle_events = []

    symbol_rows = build_edge_rows(diagnoses, "symbol", SYMBOL_EDGE_HEADERS, min_sample)
    timeframe_rows = build_edge_rows(diagnoses, "timeframe", TIMEFRAME_EDGE_HEADERS, min_sample)
    executor_rows = build_executor_blocker_rows(executor_outcomes)
    diagnosis_rows = build_diagnosis_summary_rows(diagnoses)
    recommendation_rows = build_recommendation_rows(symbol_rows, timeframe_rows, executor_rows, min_sample)
    summary = build_summary(
        since_hours=since_hours,
        signals=signals,
        diagnoses=diagnoses,
        executor_outcomes=executor_outcomes,
        lifecycle_events=lifecycle_events,
        executor_rows=executor_rows,
    )

    (out_path / SUMMARY_FILE).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(out_path / SYMBOL_EDGE_FILE, SYMBOL_EDGE_HEADERS, symbol_rows)
    write_csv(out_path / TIMEFRAME_EDGE_FILE, TIMEFRAME_EDGE_HEADERS, timeframe_rows)
    write_csv(out_path / EXECUTOR_BLOCKERS_FILE, EXECUTOR_BLOCKERS_HEADERS, executor_rows)
    write_csv(out_path / DIAGNOSIS_SUMMARY_FILE, DIAGNOSIS_SUMMARY_HEADERS, diagnosis_rows)
    write_csv(out_path / RECOMMENDATIONS_FILE, RECOMMENDATIONS_HEADERS, recommendation_rows)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily learning reports from the CandleVision SQLite database.")
    parser.add_argument("--db", default="data/signals.db", help="Path to signals SQLite database.")
    parser.add_argument("--out-dir", default="reports_learning", help="Directory for generated report files.")
    parser.add_argument("--since-hours", type=int, default=24, help="Lookback window in hours for timestamped tables.")
    parser.add_argument("--min-sample", type=int, default=5, help="Minimum sample size for recommendations.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    generate_learning_report(args.db, args.out_dir, args.since_hours, args.min_sample)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
