from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from orderflow_accum.signal_taxonomy import HIGH_POTENTIAL_KINDS, normalize_signal_kind, signal_family, signal_focus_group

SUMMARY_FILE = "learning_summary.json"
SYMBOL_EDGE_FILE = "learning_symbol_edge.csv"
TIMEFRAME_EDGE_FILE = "learning_timeframe_edge.csv"
EXECUTOR_BLOCKERS_FILE = "learning_executor_blockers.csv"
EXECUTOR_TRADES_FILE = "learning_executor_trades.csv"
DIAGNOSIS_SUMMARY_FILE = "learning_diagnosis_summary.csv"
RECOMMENDATIONS_FILE = "learning_recommendations.csv"
HIGH_POTENTIAL_FOCUS_FILE = "learning_high_potential_focus.csv"

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
    "avg_volume_impulse",
    "avg_required_volume_impulse",
    "volume_impulse_source_distribution",
    "missing_default_volume_impulse_count",
    "missing_default_volume_impulse_share",
    "volume_impulse_capped_count",
    "volume_impulse_capped_share",
    "avg_volume_impulse_raw",
    "max_volume_impulse_raw",
    "avg_volume_impulse_ratio_to_required",
    "avg_buy_flow",
    "avg_sell_flow",
    "avg_required_buy_flow",
    "avg_spread_bps",
    "avg_ask_wall_strength",
    "avg_bid_wall_strength",
    "recommendation",
]
EXECUTOR_TRADES_HEADERS = [
    "symbol",
    "timeframe",
    "side",
    "total_trades",
    "wins",
    "losses",
    "breakeven_or_flat",
    "win_rate",
    "avg_r_result",
    "total_r_result",
    "avg_max_gain_r",
    "avg_max_drawdown_r",
    "breakeven_moves",
    "top_exit_reason",
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
HIGH_POTENTIAL_FOCUS_HEADERS = [
    "signal_focus_group",
    "signal_family",
    "kind",
    "timeframe",
    "total",
    "tp2",
    "sl",
    "expired",
    "confirmed",
    "tp2_rate_closed_pct",
    "avg_score_max",
    "avg_max_gain_pct",
    "avg_max_drawdown_pct",
    "recommendation",
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


def parse_diagnostics_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def diagnostic_value(row: dict[str, Any], key: str) -> Any:
    if key in row and row.get(key) not in (None, ""):
        return row.get(key)
    return parse_diagnostics_json(row.get("diagnostics_json")).get(key)

def safe_avg(values: Iterable[Any]) -> float:
    numbers = [item for item in (safe_float(value) for value in values) if item is not None]
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def diagnostic_float(row: dict[str, Any], key: str) -> float | None:
    return safe_float(diagnostic_value(row, key))


def report_volume_impulse(row: dict[str, Any]) -> float | None:
    capped = diagnostic_float(row, "volume_impulse_capped")
    if capped is not None:
        return capped
    value = safe_float(row.get("volume_impulse"))
    if value is not None:
        return value
    return diagnostic_float(row, "volume_impulse")


def raw_volume_impulse(row: dict[str, Any]) -> float | None:
    value = safe_float(row.get("volume_impulse"))
    if value is not None:
        return value
    value = diagnostic_float(row, "volume_impulse")
    if value is not None:
        return value
    return diagnostic_float(row, "volume_impulse_raw")


def report_volume_impulse_ratio(row: dict[str, Any]) -> float | None:
    capped = diagnostic_float(row, "volume_impulse_ratio_to_required_capped")
    if capped is not None:
        return capped
    return diagnostic_float(row, "volume_impulse_ratio_to_required")


def safe_max(values: Iterable[Any]) -> float:
    numbers = [item for item in (safe_float(value) for value in values) if item is not None]
    if not numbers:
        return 0.0
    return max(numbers)

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


def signal_result(row: dict[str, Any]) -> str:
    outcome = str(row.get("outcome") or "").strip().upper()
    status = str(row.get("status") or "").strip().upper()
    return outcome or status


def signal_is_confirmed(row: dict[str, Any]) -> bool:
    return str(row.get("status") or "").strip().upper() in {"CONFIRMED", "CONFIRMED_LONG", "CONFIRMED_SHORT"}


def high_potential_recommendation(
    focus_group: str,
    *,
    total: int,
    tp2: int,
    sl: int,
    expired: int,
    tp2_rate_closed_pct: float,
    avg_max_gain_pct: float,
) -> str:
    if focus_group == "EXECUTION_STABLE":
        return "normal_tp_sl"
    if focus_group == "EXPERIMENTAL":
        return "paper_only_low_priority"
    if focus_group != "HIGH_POTENTIAL":
        return "monitor"

    expired_share = (expired / total) if total else 0.0
    if expired_share >= 0.4:
        return "extend_watch_window_or_wait_for_confirmation"
    if avg_max_gain_pct >= 3.0 and tp2_rate_closed_pct < 50.0:
        return "breakeven_first_trailing_candidate"
    if tp2_rate_closed_pct >= 50.0 and tp2 >= max(sl, 1):
        return "priority_high_potential"
    return "monitor_high_potential"


def build_high_potential_focus_rows(signals: list[dict[str, Any]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in signals:
        kind = normalize_signal_kind(row.get("kind")) or "UNKNOWN"
        if signal_focus_group(kind) != "HIGH_POTENTIAL":
            continue
        timeframe = str(row.get("timeframe") or "UNKNOWN")
        grouped[(kind, timeframe)].append(row)

    result: list[dict[str, str]] = []
    for (kind, timeframe), items in sorted(grouped.items()):
        total = len(items)
        tp2 = sum(1 for item in items if signal_result(item) == "TP2")
        sl = sum(1 for item in items if signal_result(item) == "SL")
        expired = sum(1 for item in items if signal_result(item) == "EXPIRED")
        confirmed = sum(1 for item in items if signal_is_confirmed(item))
        closed_total = tp2 + sl + expired
        tp2_rate = (tp2 / closed_total) * 100.0 if closed_total else 0.0
        avg_score_max = safe_avg(item.get("score_max") for item in items)
        avg_max_gain = safe_avg(item.get("max_gain_pct") for item in items)
        avg_max_drawdown = safe_avg(item.get("max_drawdown_pct") for item in items)
        family = signal_family(kind)
        focus_group = signal_focus_group(kind)
        result.append(
            {
                "signal_focus_group": focus_group,
                "signal_family": family,
                "kind": kind,
                "timeframe": timeframe,
                "total": str(total),
                "tp2": str(tp2),
                "sl": str(sl),
                "expired": str(expired),
                "confirmed": str(confirmed),
                "tp2_rate_closed_pct": fmt_float(tp2_rate),
                "avg_score_max": fmt_float(avg_score_max),
                "avg_max_gain_pct": fmt_float(avg_max_gain),
                "avg_max_drawdown_pct": fmt_float(avg_max_drawdown),
                "recommendation": high_potential_recommendation(
                    focus_group,
                    total=total,
                    tp2=tp2,
                    sl=sl,
                    expired=expired,
                    tp2_rate_closed_pct=tp2_rate,
                    avg_max_gain_pct=avg_max_gain,
                ),
            }
        )
    return result


def high_potential_summary_metrics(signals: list[dict[str, Any]], focus_rows: list[dict[str, str]]) -> dict[str, Any]:
    high_potential = [row for row in signals if signal_focus_group(normalize_signal_kind(row.get("kind"))) == "HIGH_POTENTIAL"]
    total = len(high_potential)
    tp2 = sum(1 for row in high_potential if signal_result(row) == "TP2")
    sl = sum(1 for row in high_potential if signal_result(row) == "SL")
    expired = sum(1 for row in high_potential if signal_result(row) == "EXPIRED")
    closed_total = tp2 + sl + expired
    recommendation_counts = Counter(row["recommendation"] for row in focus_rows)
    kind_totals = Counter(normalize_signal_kind(row.get("kind")) for row in high_potential)
    return {
        "high_potential_total": total,
        "high_potential_tp2": tp2,
        "high_potential_sl": sl,
        "high_potential_expired": expired,
        "high_potential_tp2_rate_closed_pct": round((tp2 / closed_total) * 100.0, 2) if closed_total else 0.0,
        "top_high_potential_kinds": [kind for kind, count in sorted(kind_totals.items(), key=lambda item: (-item[1], item[0])) if kind in HIGH_POTENTIAL_KINDS][:10],
        "high_potential_recommendations_count": dict(sorted(recommendation_counts.items())),
    }


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


def executor_blocker_recommendation(reason: str, metrics: dict[str, float]) -> str:
    if reason == "entry_blocked_volume_impulse":
        missing_share = metrics.get("missing_default_volume_impulse_share", 0.0)
        if missing_share >= 0.5:
            return "fix snapshot mapping before changing thresholds: missing_default volume_impulse dominates"
        capped_share = metrics.get("volume_impulse_capped_share", 0.0)
        if capped_share >= 0.25:
            return "volume impulse outliers were capped for reporting; review baseline stability"
        avg_ratio = metrics.get("avg_volume_impulse_ratio_to_required", 0.0)
        avg_volume = metrics.get("avg_volume_impulse", 0.0)
        avg_required = metrics.get("avg_required_volume_impulse", 0.0)
        if avg_ratio >= 0.85 or (avg_required > 0 and avg_volume >= avg_required * 0.85):
            return "review volume_impulse threshold sensitivity: average is close to required"
        if avg_required > 0 or avg_ratio > 0:
            if metrics.get("known_volume_impulse_source_share", 0.0) <= 0:
                return "review snapshot mapping or confirm market weakness: average is far below required"
            return "confirm market weakness before changing thresholds: real volume impulse is far below required"
        return "review volume_impulse threshold or snapshot mapping"

    if reason == "entry_blocked_buy_flow":
        avg_buy_flow = metrics.get("avg_buy_flow", 0.0)
        avg_required = metrics.get("avg_required_buy_flow", 0.0)
        if avg_required > 0 and avg_buy_flow >= avg_required * 0.85:
            return "review flow_ratio sensitivity: average buy flow is close to required"
        if avg_required > 0:
            return "keep strict flow filter: average buy flow is far below required"
        return "review buy_flow/sell_flow ratio"

    if reason in BLOCKER_REASONS:
        return BLOCKER_REASONS[reason][1]
    return "review blocker frequency" if reason.startswith("entry_blocked") else "keep monitoring"


def build_executor_blocker_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        reason = str(row.get("reason") or "UNKNOWN")
        grouped[reason].append(row)

    result: list[dict[str, Any]] = []
    for reason, items in sorted(grouped.items(), key=lambda pair: (-len(pair[1]), pair[0])):
        source_counts = Counter(str(diagnostic_value(item, "volume_impulse_source") or "UNKNOWN") for item in items)
        missing_default_count = source_counts.get("missing_default", 0)
        capped_count = sum(1 for item in items if diagnostic_value(item, "volume_impulse_was_capped") is True)
        metrics = {
            "avg_volume_impulse": safe_avg(report_volume_impulse(item) for item in items),
            "avg_required_volume_impulse": safe_avg(item.get("required_volume_impulse") for item in items),
            "missing_default_volume_impulse_count": float(missing_default_count),
            "missing_default_volume_impulse_share": rate(missing_default_count, len(items)),
            "volume_impulse_capped_count": float(capped_count),
            "volume_impulse_capped_share": rate(capped_count, len(items)),
            "avg_volume_impulse_raw": safe_avg(raw_volume_impulse(item) for item in items),
            "max_volume_impulse_raw": safe_max(raw_volume_impulse(item) for item in items),
            "avg_volume_impulse_ratio_to_required": safe_avg(report_volume_impulse_ratio(item) for item in items),
            "known_volume_impulse_source_share": rate(
                sum(1 for item in items if diagnostic_value(item, "volume_impulse_source") not in (None, "")),
                len(items),
            ),
            "avg_buy_flow": safe_avg(item.get("buy_flow") for item in items),
            "avg_sell_flow": safe_avg(item.get("sell_flow") for item in items),
            "avg_required_buy_flow": safe_avg(item.get("required_buy_flow") for item in items),
            "avg_spread_bps": safe_avg(item.get("spread_bps") for item in items),
            "avg_ask_wall_strength": safe_avg(item.get("ask_wall_strength") for item in items),
            "avg_bid_wall_strength": safe_avg(item.get("bid_wall_strength") for item in items),
        }
        recommendation = executor_blocker_recommendation(reason, metrics)
        result.append(
            {
                "reason": reason,
                "total": str(len(items)),
                "symbols_count": str(len({str(item.get("symbol") or "UNKNOWN") for item in items})),
                "avg_max_gain_r": fmt_float(safe_avg(item.get("max_gain_r") for item in items)),
                "avg_max_drawdown_r": fmt_float(safe_avg(item.get("max_drawdown_r") for item in items)),
                "volume_impulse_source_distribution": ";".join(
                    f"{source}:{count}" for source, count in sorted(source_counts.items())
                ),
                **{key: fmt_float(value) for key, value in metrics.items()},
                "recommendation": recommendation,
            }
        )
    return result



def executor_trade_recommendation(items: list[dict[str, Any]], min_sample: int) -> str:
    sample = len(items)
    avg_r = safe_avg(item.get("r_result") for item in items)
    breakeven_moves = sum(1 for item in items if int(item.get("moved_to_breakeven") or 0) == 1)
    near_flat = sum(1 for item in items if abs(safe_float(item.get("r_result")) or 0.0) <= 0.1)
    exit_reasons = Counter(str(item.get("exit_reason") or "UNKNOWN") for item in items)
    top_reason, top_count = exit_reasons.most_common(1)[0] if exit_reasons else ("", 0)

    if top_reason == "exit_ask_wall_pressure" and top_count > 0:
        stopped_count = exit_reasons.get("exit_stop_loss_hit", 0)
        if stopped_count == 0 or top_count >= stopped_count:
            return "useful protective exit: ask wall pressure often exits before SL"
    if sample >= min_sample and top_reason == "exit_stop_loss_hit" and top_count / sample >= 0.5:
        return "review SL placement: exit_stop_loss_hit dominates"
    if sample >= min_sample and breakeven_moves / sample >= 0.5 and near_flat / sample >= 0.5:
        return "review trailing logic: many breakeven moves exit near 0R"
    if sample >= min_sample and avg_r < 0:
        return "review exit/entry timing: average R is negative"
    if sample < min_sample:
        return "collect more samples"
    return "keep monitoring"


def build_executor_trade_rows(rows: list[dict[str, Any]], min_sample: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row.get("symbol") or "UNKNOWN"),
            str(row.get("timeframe") or "UNKNOWN"),
            str(row.get("side") or "UNKNOWN"),
        )
        grouped[key].append(row)

    result: list[dict[str, Any]] = []
    for (symbol, timeframe, side), items in sorted(grouped.items()):
        r_values = [safe_float(item.get("r_result")) for item in items]
        wins = sum(1 for value in r_values if value is not None and value > 0)
        losses = sum(1 for value in r_values if value is not None and value < 0)
        breakeven_or_flat = sum(1 for value in r_values if value is None or value == 0)
        total_r = sum(value for value in r_values if value is not None)
        exit_reasons = Counter(str(item.get("exit_reason") or "UNKNOWN") for item in items)
        top_exit_reason = exit_reasons.most_common(1)[0][0] if exit_reasons else ""
        output = {
            "symbol": symbol,
            "timeframe": timeframe,
            "side": side,
            "total_trades": str(len(items)),
            "wins": str(wins),
            "losses": str(losses),
            "breakeven_or_flat": str(breakeven_or_flat),
            "win_rate": fmt_float(rate(wins, len(items))),
            "avg_r_result": fmt_float(safe_avg(item.get("r_result") for item in items)),
            "total_r_result": fmt_float(total_r),
            "avg_max_gain_r": fmt_float(safe_avg(item.get("max_gain_r") for item in items)),
            "avg_max_drawdown_r": fmt_float(safe_avg(item.get("max_drawdown_r") for item in items)),
            "breakeven_moves": str(sum(1 for item in items if int(item.get("moved_to_breakeven") or 0) == 1)),
            "top_exit_reason": top_exit_reason,
            "recommendation": executor_trade_recommendation(items, min_sample),
        }
        result.append({header: output.get(header, "") for header in EXECUTOR_TRADES_HEADERS})
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
            if reason == "entry_blocked_volume_impulse":
                recommendation = str(row.get("recommendation") or recommendation)
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
    executor_trades: list[dict[str, Any]],
    high_potential_focus_rows: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    outcome_counts = Counter(str(row.get("outcome") or "UNKNOWN").upper() for row in diagnoses)
    executor_action_counts = Counter(str(row.get("action") or "UNKNOWN") for row in executor_outcomes)
    executor_trade_exit_reason_counts = Counter(str(row.get("exit_reason") or "UNKNOWN") for row in executor_trades)
    executor_trade_r_values = [value for value in (safe_float(row.get("r_result")) for row in executor_trades) if value is not None]
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

    high_potential_metrics = high_potential_summary_metrics(signals, high_potential_focus_rows or [])

    return {
        "generated_at": utc_iso_now(),
        "since_hours": since_hours,
        "total_signals": len(signals),
        "total_diagnoses": len(diagnoses),
        "total_executor_decisions": len(executor_outcomes),
        "total_lifecycle_events": len(lifecycle_events),
        "total_executor_trades": len(executor_trades),
        "executor_trades_total_r": sum(executor_trade_r_values),
        "executor_trades_avg_r": safe_avg(executor_trade_r_values),
        "executor_trade_exit_reason_counts": dict(sorted(executor_trade_exit_reason_counts.items())),
        "outcome_counts": dict(sorted(outcome_counts.items())),
        "executor_action_counts": dict(sorted(executor_action_counts.items())),
        "top_tp_symbols": top_tp_symbols,
        "top_sl_symbols": top_sl_symbols,
        "top_executor_blockers": top_executor_blockers,
        "high_level_notes": high_level_notes,
        **high_potential_metrics,
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
            executor_trades = select_rows(conn, "executor_trades", since)
        finally:
            conn.close()
    else:
        signals = []
        diagnoses = []
        executor_outcomes = []
        lifecycle_events = []
        executor_trades = []

    symbol_rows = build_edge_rows(diagnoses, "symbol", SYMBOL_EDGE_HEADERS, min_sample)
    timeframe_rows = build_edge_rows(diagnoses, "timeframe", TIMEFRAME_EDGE_HEADERS, min_sample)
    executor_rows = build_executor_blocker_rows(executor_outcomes)
    diagnosis_rows = build_diagnosis_summary_rows(diagnoses)
    executor_trade_rows = build_executor_trade_rows(executor_trades, min_sample)
    recommendation_rows = build_recommendation_rows(symbol_rows, timeframe_rows, executor_rows, min_sample)
    high_potential_focus_rows = build_high_potential_focus_rows(signals)
    summary = build_summary(
        since_hours=since_hours,
        signals=signals,
        diagnoses=diagnoses,
        executor_outcomes=executor_outcomes,
        lifecycle_events=lifecycle_events,
        executor_rows=executor_rows,
        executor_trades=executor_trades,
        high_potential_focus_rows=high_potential_focus_rows,
    )

    (out_path / SUMMARY_FILE).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_csv(out_path / SYMBOL_EDGE_FILE, SYMBOL_EDGE_HEADERS, symbol_rows)
    write_csv(out_path / TIMEFRAME_EDGE_FILE, TIMEFRAME_EDGE_HEADERS, timeframe_rows)
    write_csv(out_path / EXECUTOR_BLOCKERS_FILE, EXECUTOR_BLOCKERS_HEADERS, executor_rows)
    write_csv(out_path / EXECUTOR_TRADES_FILE, EXECUTOR_TRADES_HEADERS, executor_trade_rows)
    write_csv(out_path / DIAGNOSIS_SUMMARY_FILE, DIAGNOSIS_SUMMARY_HEADERS, diagnosis_rows)
    write_csv(out_path / RECOMMENDATIONS_FILE, RECOMMENDATIONS_HEADERS, recommendation_rows)
    write_csv(out_path / HIGH_POTENTIAL_FOCUS_FILE, HIGH_POTENTIAL_FOCUS_HEADERS, high_potential_focus_rows)
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
