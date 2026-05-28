from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

WATCHLIST_STATUSES = {"WATCHING", "ACCUMULATION", "PRE_IMPULSE"}
WATCHLIST_KINDS = {"ACCUMULATION_WATCH", "ABSORPTION_ZONE", "PRE_IMPULSE_ZONE"}


def _winner_group(outcome: str | None) -> str:
    o = (outcome or "").upper()
    if o == "TP2":
        return "TP2_WINNER"
    if o == "TP1":
        return "TP1_WINNER"
    if o == "SL":
        return "SL_LOSER"
    if o == "EXPIRED":
        return "EXPIRED"
    if o == "AMBIGUOUS":
        return "AMBIGUOUS"
    return "PENDING"


def _parse_reasons(raw: str | None) -> list[str]:
    if not raw:
        return []
    s = raw.strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            items = json.loads(s)
            if isinstance(items, list):
                return [str(x).strip() for x in items if str(x).strip()]
        except Exception:
            pass
    if "|" in s:
        return [x.strip() for x in s.split("|") if x.strip()]
    return [s]


def _to_float(v: object) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _avg(vals: list[float | None]) -> float | None:
    x = [v for v in vals if v is not None]
    return (sum(x) / len(x)) if x else None


def _bucket_time(minutes: float | None) -> str:
    if minutes is None:
        return "never"
    if minutes <= 15:
        return "0-15m"
    if minutes <= 30:
        return "15-30m"
    if minutes <= 60:
        return "30-60m"
    if minutes <= 180:
        return "1-3h"
    if minutes <= 360:
        return "3-6h"
    if minutes <= 720:
        return "6-12h"
    if minutes <= 1440:
        return "12-24h"
    return "24h+"


def _write_csv(path: Path, rows: list[dict], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in headers})


def build_reports(db_path: Path, out_dir: Path) -> dict[str, Path]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM signals").fetchall()
    conn.close()

    analysis: list[dict] = []
    for r in rows:
        status = str(r["status"] or "")
        kind = str(r["kind"] or "")
        if status.upper() not in WATCHLIST_STATUSES and kind.upper() not in WATCHLIST_KINDS:
            continue

        outcome = str(r["outcome"] or r["status"] or "PENDING").upper()
        tp1 = _to_float(r["time_to_tp1_minutes"])
        tp2 = _to_float(r["time_to_tp2_minutes"])
        tsl = _to_float(r["time_to_sl_minutes"])
        t05 = tp1
        t1r = tp1
        analysis.append(
            {
                "symbol": r["symbol"],
                "market": r["market"],
                "timeframe": r["timeframe"],
                "side": r["side"],
                "kind": r["kind"],
                "status": r["status"],
                "outcome": outcome,
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
                "repeat_count": r["repeat_count"],
                "score_first": r["score_first"],
                "score_last": r["score_last"],
                "score_max": r["score_max"],
                "entry": r["entry"],
                "stop_loss": r["stop_loss"],
                "take_profit_1": r["take_profit_1"],
                "take_profit_2": r["take_profit_2"],
                "max_gain_pct": r["max_gain_pct"],
                "max_drawdown_pct": r["max_drawdown_pct"],
                "time_to_tp1_minutes": tp1,
                "time_to_tp2_minutes": tp2,
                "time_to_sl_minutes": tsl,
                "time_to_max_gain_minutes": None,
                "drawdown_before_tp_pct": r["max_drawdown_pct"],
                "btc_regime": (json.loads(r["meta"]).get("btc_regime") if r["meta"] else None) if str(r["meta"] or "").startswith("{") else None,
                "reasons_first": r["reasons_first"],
                "reasons_last": r["reasons_last"],
                "winner_group": _winner_group(outcome),
                "time_to_0_5R_minutes": t05,
                "time_to_1R_minutes": t1r,
                "time_to_1_5R_minutes": tp2,
                "time_to_2R_minutes": tp2,
            }
        )

    analysis_headers = list(analysis[0].keys()) if analysis else [
        "symbol","market","timeframe","side","kind","status","outcome","first_seen","last_seen",
        "repeat_count","score_first","score_last","score_max","entry","stop_loss","take_profit_1","take_profit_2",
        "max_gain_pct","max_drawdown_pct","time_to_tp1_minutes","time_to_tp2_minutes","time_to_sl_minutes",
        "time_to_max_gain_minutes","drawdown_before_tp_pct","btc_regime","reasons_first","reasons_last","winner_group",
        "time_to_0_5R_minutes","time_to_1R_minutes","time_to_1_5R_minutes","time_to_2R_minutes"
    ]
    paths = {"analysis": out_dir / "watchlist_analysis.csv"}
    _write_csv(paths["analysis"], analysis, analysis_headers)

    # reason edge
    grouped: dict[str, list[dict]] = defaultdict(list)
    for a in analysis:
        reasons = set(_parse_reasons(a.get("reasons_first")) + _parse_reasons(a.get("reasons_last")))
        for rs in reasons:
            grouped[rs].append(a)

    reason_rows = []
    for reason, items in sorted(grouped.items()):
        total = len(items)
        tp = sum(1 for i in items if i["outcome"] in {"TP1", "TP2"})
        sl = sum(1 for i in items if i["outcome"] == "SL")
        expired = sum(1 for i in items if i["outcome"] == "EXPIRED")
        amb = sum(1 for i in items if i["outcome"] == "AMBIGUOUS")
        reason_rows.append({
            "reason": reason, "total": total, "tp_count": tp, "sl_count": sl,
            "expired_count": expired, "ambiguous_count": amb,
            "tp_rate": tp / total if total else 0, "sl_rate": sl / total if total else 0,
            "avg_max_gain_pct": _avg([_to_float(i["max_gain_pct"]) for i in items]),
            "avg_max_drawdown_pct": _avg([_to_float(i["max_drawdown_pct"]) for i in items]),
            "avg_time_to_0_5R": _avg([_to_float(i["time_to_0_5R_minutes"]) for i in items]),
            "avg_time_to_1R": _avg([_to_float(i["time_to_1R_minutes"]) for i in items]),
            "avg_time_to_tp1": _avg([_to_float(i["time_to_tp1_minutes"]) for i in items]),
            "avg_score_first": _avg([_to_float(i["score_first"]) for i in items]),
            "avg_score_max": _avg([_to_float(i["score_max"]) for i in items]),
        })
    paths["reason"] = out_dir / "watchlist_reason_edge.csv"
    _write_csv(paths["reason"], reason_rows, list(reason_rows[0].keys()) if reason_rows else ["reason","total","tp_count","sl_count","expired_count","ambiguous_count","tp_rate","sl_rate","avg_max_gain_pct","avg_max_drawdown_pct","avg_time_to_0_5R","avg_time_to_1R","avg_time_to_tp1","avg_score_first","avg_score_max"])

    # time to move
    by_bucket: dict[str, list[dict]] = defaultdict(list)
    for a in analysis:
        by_bucket[_bucket_time(_to_float(a.get("time_to_0_5R_minutes")))].append(a)
    t_rows = []
    for b, items in by_bucket.items():
        total = len(items)
        tp = sum(1 for i in items if i["outcome"] in {"TP1", "TP2"})
        sl = sum(1 for i in items if i["outcome"] == "SL")
        t_rows.append({
            "bucket": b, "total": total, "tp_rate": tp / total if total else 0, "sl_rate": sl / total if total else 0,
            "avg_max_gain_pct": _avg([_to_float(i["max_gain_pct"]) for i in items]),
            "avg_max_drawdown_pct": _avg([_to_float(i["max_drawdown_pct"]) for i in items]),
            "avg_score_first": _avg([_to_float(i["score_first"]) for i in items]),
            "avg_score_max": _avg([_to_float(i["score_max"]) for i in items]),
        })
    paths["time_to_move"] = out_dir / "watchlist_time_to_move.csv"
    _write_csv(paths["time_to_move"], t_rows, list(t_rows[0].keys()) if t_rows else ["bucket","total","tp_rate","sl_rate","avg_max_gain_pct","avg_max_drawdown_pct","avg_score_first","avg_score_max"])

    # timeframe edge
    tf_map: dict[str, list[dict]] = defaultdict(list)
    for a in analysis:
        tf_map[str(a["timeframe"])].append(a)
    tf_rows = []
    for tf, items in tf_map.items():
        total = len(items)
        tp = sum(1 for i in items if i["outcome"] in {"TP1", "TP2"})
        sl = sum(1 for i in items if i["outcome"] == "SL")
        exp = sum(1 for i in items if i["outcome"] == "EXPIRED")
        tf_rows.append({
            "timeframe": tf, "total": total, "tp_rate": tp / total if total else 0,
            "sl_rate": sl / total if total else 0, "expired_rate": exp / total if total else 0,
            "avg_max_gain_pct": _avg([_to_float(i["max_gain_pct"]) for i in items]),
            "avg_max_drawdown_pct": _avg([_to_float(i["max_drawdown_pct"]) for i in items]),
            "avg_time_to_0_5R": _avg([_to_float(i["time_to_0_5R_minutes"]) for i in items]),
            "avg_time_to_1R": _avg([_to_float(i["time_to_1R_minutes"]) for i in items]),
            "avg_time_to_tp1": _avg([_to_float(i["time_to_tp1_minutes"]) for i in items]),
        })
    paths["timeframe"] = out_dir / "watchlist_timeframe_edge.csv"
    _write_csv(paths["timeframe"], tf_rows, list(tf_rows[0].keys()) if tf_rows else ["timeframe","total","tp_rate","sl_rate","expired_rate","avg_max_gain_pct","avg_max_drawdown_pct","avg_time_to_0_5R","avg_time_to_1R","avg_time_to_tp1"])

    # btc regime edge
    br_map: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for a in analysis:
        br_map[(str(a.get("btc_regime") or "UNKNOWN"), str(a.get("side") or "UNKNOWN"))].append(a)
    br_rows = []
    for (regime, side), items in br_map.items():
        total = len(items)
        tp = sum(1 for i in items if i["outcome"] in {"TP1", "TP2"})
        sl = sum(1 for i in items if i["outcome"] == "SL")
        br_rows.append({
            "btc_regime": regime, "side": side, "total": total,
            "tp_rate": tp / total if total else 0, "sl_rate": sl / total if total else 0,
            "avg_max_gain_pct": _avg([_to_float(i["max_gain_pct"]) for i in items]),
            "avg_max_drawdown_pct": _avg([_to_float(i["max_drawdown_pct"]) for i in items]),
        })
    paths["btc_regime"] = out_dir / "watchlist_btc_regime_edge.csv"
    _write_csv(paths["btc_regime"], br_rows, list(br_rows[0].keys()) if br_rows else ["btc_regime","side","total","tp_rate","sl_rate","avg_max_gain_pct","avg_max_drawdown_pct"])

    return paths


def main() -> None:
    p = argparse.ArgumentParser(description="Watchlist transition study from signals.db")
    p.add_argument("--db", default="data/signals.db")
    p.add_argument("--out", default=None, help="Output path for primary analysis csv")
    p.add_argument("--out-dir", default="reports")
    args = p.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"DB not found: {db}")
        return

    out_dir = Path(args.out_dir)
    paths = build_reports(db, out_dir)
    if args.out:
        custom = Path(args.out)
        custom.parent.mkdir(parents=True, exist_ok=True)
        custom.write_text(paths["analysis"].read_text(encoding="utf-8"), encoding="utf-8")
        print(f"analysis={custom}")
    else:
        print(f"analysis={paths['analysis']}")
    for k, v in paths.items():
        if k != "analysis":
            print(f"{k}={v}")


if __name__ == "__main__":
    main()
