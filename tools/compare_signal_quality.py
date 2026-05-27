from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


FINAL_STATUSES = {"TP1", "TP2", "SL", "AMBIGUOUS", "EXPIRED", "FAILED"}


def _load_metrics(db_path: Path) -> dict:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT status, reasons_last, timeframe, repeat_count FROM signals"
        ).fetchall()
    finally:
        conn.close()

    total = len(rows)
    final_rows = [r for r in rows if str(r["status"] or "").upper() in FINAL_STATUSES]
    tp = sum(1 for r in final_rows if str(r["status"]).upper() in {"TP1", "TP2"})
    sl = sum(1 for r in final_rows if str(r["status"]).upper() == "SL")
    ambiguous = sum(1 for r in final_rows if str(r["status"]).upper() == "AMBIGUOUS")
    win_rate = (tp / max(tp + sl, 1)) * 100.0

    tf_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    repeats = []

    for r in rows:
        tf = str(r["timeframe"] or "unknown")
        tf_counts[tf] = tf_counts.get(tf, 0) + 1
        repeats.append(int(r["repeat_count"] or 0))
        try:
            reasons = json.loads(r["reasons_last"] or "[]")
            if isinstance(reasons, list):
                for reason in reasons:
                    key = str(reason)
                    reason_counts[key] = reason_counts.get(key, 0) + 1
        except Exception:
            pass

    avg_repeat = (sum(repeats) / max(len(repeats), 1)) if repeats else 0.0
    duplicate_ratio = 0.0
    if repeats:
        duplicate_ratio = sum(max(v - 1, 0) for v in repeats) / max(sum(repeats), 1)

    return {
        "db": str(db_path),
        "signals_total": total,
        "final_total": len(final_rows),
        "tp": tp,
        "sl": sl,
        "ambiguous": ambiguous,
        "win_rate_pct": round(win_rate, 2),
        "avg_repeat_count": round(avg_repeat, 3),
        "duplicate_ratio": round(duplicate_ratio, 4),
        "top_timeframes": sorted(tf_counts.items(), key=lambda x: x[1], reverse=True)[:5],
        "top_reasons": sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:10],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare signal quality between two signals.db snapshots")
    parser.add_argument("--before", required=True, help="Path to baseline signals.db")
    parser.add_argument("--after", required=True, help="Path to new signals.db")
    args = parser.parse_args()

    before = Path(args.before)
    after = Path(args.after)
    if not before.exists() or not after.exists():
        raise SystemExit("Both --before and --after paths must exist")

    m_before = _load_metrics(before)
    m_after = _load_metrics(after)

    compare = {
        "before": m_before,
        "after": m_after,
        "delta": {
            "win_rate_pct": round(m_after["win_rate_pct"] - m_before["win_rate_pct"], 2),
            "duplicate_ratio": round(m_after["duplicate_ratio"] - m_before["duplicate_ratio"], 4),
            "avg_repeat_count": round(m_after["avg_repeat_count"] - m_before["avg_repeat_count"], 3),
            "signals_total": m_after["signals_total"] - m_before["signals_total"],
        },
    }
    print(json.dumps(compare, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
