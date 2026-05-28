from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _parse_ts(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def main() -> int:
    p = argparse.ArgumentParser(description="Analyze TP/SL hit distribution from accumulation_signals.csv")
    p.add_argument("--file", default="accumulation_signals.csv")
    p.add_argument("--since-hours", type=float, default=36.0)
    p.add_argument("--dedupe-minutes", type=int, default=360)
    p.add_argument("--category", default="linear")
    args = p.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"File not found: {path}")
        return 1

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=args.since_hours)
    dedupe_delta = timedelta(minutes=args.dedupe_minutes)

    rows = list(csv.DictReader(path.open("r", encoding="utf-8", newline="")))
    filtered: list[dict[str, str]] = []
    for row in rows:
        ts = _parse_ts(row.get("timestamp", ""))
        if ts and ts >= cutoff:
            filtered.append(row)

    filtered.sort(key=lambda r: _parse_ts(r.get("timestamp", "")) or datetime.min.replace(tzinfo=timezone.utc))

    deduped: list[dict[str, str]] = []
    last_seen: dict[tuple[str, str], datetime] = {}
    for row in filtered:
        symbol = (row.get("symbol") or "").upper()
        tf = (row.get("timeframe") or row.get("tf") or "").lower()
        ts = _parse_ts(row.get("timestamp", ""))
        if not symbol or not ts:
            continue
        key = (symbol, tf)
        prev = last_seen.get(key)
        if prev and (ts - prev) < dedupe_delta:
            continue
        last_seen[key] = ts
        deduped.append(row)

    outcome_counter = Counter((r.get("status") or "UNKNOWN").upper() for r in deduped)
    total = sum(outcome_counter.values())
    tp = outcome_counter.get("TP1", 0) + outcome_counter.get("TP2", 0) + outcome_counter.get("TP", 0)
    sl = outcome_counter.get("SL", 0)

    print(f"File: {path}")
    print(f"Category: {args.category}")
    print(f"Rows: raw={len(rows)} filtered={len(filtered)} deduped={len(deduped)}")
    print(f"Outcomes: {dict(outcome_counter)}")
    if total:
        print(f"TP-rate: {tp / total * 100:.2f}%")
        print(f"SL-rate: {sl / total * 100:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
