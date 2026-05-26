from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path


def _bucket(score: float) -> str:
    if score < 5:
        return "<5"
    if score < 7:
        return "5-7"
    if score < 9:
        return "7-9"
    if score < 11:
        return "9-11"
    return "11+"


def _status_win_loss(status: str) -> str:
    s = (status or "").upper()
    if s in {"TP1", "TP2"}:
        return "TP"
    if s == "SL":
        return "SL"
    return "OTHER"


def main() -> None:
    parser = argparse.ArgumentParser(description="Signal performance report from data/signals.db")
    parser.add_argument("--db", default="data/signals.db")
    args = parser.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"DB not found: {db}")
        return

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM signals").fetchall()
    conn.close()

    if not rows:
        print("No signals found.")
        return

    reason_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
    score_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
    tf_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
    kind_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
    source_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
    reason_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "mfe": 0.0, "mae": 0.0})
    score_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})
    tf_stats: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0, "tp": 0, "sl": 0, "pending": 0, "mfe": 0.0, "mae": 0.0})


    for r in rows:
        status = (r["status"] or "PENDING").upper()
        winloss = _status_win_loss(status)
        score_last = float(r["score_last"] or 0.0)
        bucket = _bucket(score_last)
        tf = str(r["timeframe"] or "1")
        kind = str(r["kind"] or "UNKNOWN")
        source = str(r["source"] or "UNKNOWN")
        mfe = float(r["max_gain_pct"] or 0.0)
        mae = float(r["max_drawdown_pct"] or 0.0)

        reasons_last = r["reasons_last"] or "[]"
        try:
            import json
            reasons = json.loads(reasons_last)
            if not isinstance(reasons, list):
                reasons = []
        except Exception:
            reasons = []

        for reason in reasons:
            key = str(reason)
            st = reason_stats[key]
            st["total"] += 1
            st["mfe"] += mfe
            st["mae"] += mae
            if winloss == "TP":
                st["tp"] += 1
            elif winloss == "SL":
                st["sl"] += 1
            else:
                st["pending"] += 1

        for st in (score_stats[bucket], tf_stats[tf], kind_stats[kind], source_stats[source]):
        for st in (score_stats[bucket], tf_stats[tf]):

            st["total"] += 1
            st["mfe"] += mfe
            st["mae"] += mae
            if winloss == "TP":
                st["tp"] += 1
            elif winloss == "SL":
                st["sl"] += 1
            else:
                st["pending"] += 1

    print("\n=== REASON REPORT ===")
    print("reason\ttotal\ttp\tsl\twin_rate\tavg_mfe\tavg_mae")
    for reason, st in sorted(reason_stats.items(), key=lambda x: x[1]["total"], reverse=True)[:80]:
        total = st["total"] or 1
        win_rate = (st["tp"] / max(st["tp"] + st["sl"], 1)) * 100.0
        print(f"{reason}\t{int(st['total'])}\t{int(st['tp'])}\t{int(st['sl'])}\t{win_rate:.2f}%\t{st['mfe']/total:.3f}\t{st['mae']/total:.3f}")

    print("\n=== SCORE BUCKET REPORT ===")
    print("bucket\ttotal\ttp\tsl\tpending\twin_rate\tavg_mfe\tavg_mae")
    for b in ["<5", "5-7", "7-9", "9-11", "11+"]:
        st = score_stats[b]
        total = st["total"] or 1
        win_rate = (st["tp"] / max(st["tp"] + st["sl"], 1)) * 100.0
        print(f"{b}\t{int(st['total'])}\t{int(st['tp'])}\t{int(st['sl'])}\t{int(st['pending'])}\t{win_rate:.2f}%\t{st['mfe']/total:.3f}\t{st['mae']/total:.3f}")

    print("\n=== TIMEFRAME REPORT ===")
    print("tf\ttotal\ttp\tsl\tpending\twin_rate\tavg_mfe\tavg_mae")
    for tf, st in sorted(tf_stats.items(), key=lambda x: x[1]["total"], reverse=True):
        total = st["total"] or 1
        win_rate = (st["tp"] / max(st["tp"] + st["sl"], 1)) * 100.0
        print(f"{tf}\t{int(st['total'])}\t{int(st['tp'])}\t{int(st['sl'])}\t{int(st['pending'])}\t{win_rate:.2f}%\t{st['mfe']/total:.3f}\t{st['mae']/total:.3f}")

    print("\n=== KIND REPORT ===")
    print("kind\ttotal\ttp\tsl\tpending\twin_rate\tavg_mfe\tavg_mae")
    for kind, st in sorted(kind_stats.items(), key=lambda x: x[1]["total"], reverse=True):
        total = st["total"] or 1
        win_rate = (st["tp"] / max(st["tp"] + st["sl"], 1)) * 100.0
        print(f"{kind}\t{int(st['total'])}\t{int(st['tp'])}\t{int(st['sl'])}\t{int(st['pending'])}\t{win_rate:.2f}%\t{st['mfe']/total:.3f}\t{st['mae']/total:.3f}")

    print("\n=== SOURCE REPORT ===")
    print("source\ttotal\ttp\tsl\tpending\twin_rate\tavg_mfe\tavg_mae")
    for source, st in sorted(source_stats.items(), key=lambda x: x[1]["total"], reverse=True):
        total = st["total"] or 1
        win_rate = (st["tp"] / max(st["tp"] + st["sl"], 1)) * 100.0
        print(f"{source}\t{int(st['total'])}\t{int(st['tp'])}\t{int(st['sl'])}\t{int(st['pending'])}\t{win_rate:.2f}%\t{st['mfe']/total:.3f}\t{st['mae']/total:.3f}")


if __name__ == "__main__":
    main()
