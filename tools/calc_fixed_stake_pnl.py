from __future__ import annotations

import argparse
import csv
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any


TERMINAL_STATES = {"EXITED", "CLOSED", "TP1", "TP2", "SL", "AMBIGUOUS", "EXPIRED"}
ACTIVE_STATES = {"ENTERED", "PROTECT_BREAKEVEN", "TRAILING_PROFIT", "TRADE_WATCH"}
ACTIVE_ACTIONS = {"HOLD", "WATCH"}


def fnum(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def first_col(cols: set[str], names: list[str]) -> str | None:
    for n in names:
        if n in cols:
            return n
    return None


def load_executor_outcomes(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    cols = columns(conn, "executor_outcomes")
    order_col = "updated_at" if "updated_at" in cols else "created_at"

    rows = [dict(r) for r in conn.execute(f"""
        SELECT *
        FROM executor_outcomes
        ORDER BY {order_col} ASC
    """).fetchall()]

    latest_by_key: dict[str, dict[str, Any]] = {}
    for r in rows:
        key = str(r.get("signal_key") or r.get("id") or "")
        if not key:
            key = f"{r.get('symbol')}|{r.get('side')}|{r.get('entry_price')}|{r.get('created_at')}"
        latest_by_key[key] = r

    stats = {
        "raw_rows": len(rows),
        "unique_keys": len(latest_by_key),
        "skipped_active_or_watch": 0,
        "skipped_missing_price": 0,
        "skipped_unknown_side": 0,
    }

    trades: list[dict[str, Any]] = []

    for key, r in latest_by_key.items():
        state = str(r.get("state") or "").upper()
        action = str(r.get("action") or "").upper()

        if state in ACTIVE_STATES or action in ACTIVE_ACTIONS:
            stats["skipped_active_or_watch"] += 1
            continue

        exit_price = fnum(r.get("exit_price"))
        entry_price = fnum(r.get("entry_price"))
        if entry_price is None or entry_price <= 0 or exit_price is None or exit_price <= 0:
            stats["skipped_missing_price"] += 1
            continue

        side = str(r.get("side") or "").strip()
        if side not in {"Buy", "Sell"}:
            stats["skipped_unknown_side"] += 1
            continue

        trades.append({
            "source": "executor_outcomes",
            "key": key,
            "symbol": r.get("symbol"),
            "side": side,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "state": r.get("state"),
            "action": r.get("action"),
            "reason": r.get("reason") or r.get("exit_reason"),
            "created_at": r.get("created_at"),
            "closed_at": r.get("updated_at") or r.get("created_at"),
        })

    return trades, stats


def load_executor_trades(conn: sqlite3.Connection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    cols = columns(conn, "executor_trades")

    entry_col = first_col(cols, ["entry_price", "entry"])
    exit_col = first_col(cols, ["exit_price", "close_price", "closed_price", "exit"])
    side_col = first_col(cols, ["side", "direction"])
    symbol_col = first_col(cols, ["symbol", "ticker"])
    state_col = first_col(cols, ["state", "status"])
    action_col = first_col(cols, ["action"])
    key_col = first_col(cols, ["signal_key", "trade_key", "id"])
    created_col = first_col(cols, ["created_at", "opened_at", "entry_time"])
    closed_col = first_col(cols, ["closed_at", "exit_time", "updated_at", "created_at"])
    reason_col = first_col(cols, ["reason", "exit_reason", "status"])

    stats = {
        "raw_rows": 0,
        "unique_keys": 0,
        "skipped_active_or_watch": 0,
        "skipped_missing_price": 0,
        "skipped_unknown_side": 0,
    }

    if not entry_col or not exit_col or not side_col:
        return [], stats

    rows = [dict(r) for r in conn.execute("SELECT * FROM executor_trades").fetchall()]
    stats["raw_rows"] = len(rows)

    latest_by_key: dict[str, dict[str, Any]] = {}
    for idx, r in enumerate(rows):
        key = str(r.get(key_col) if key_col else "") or str(idx)
        latest_by_key[key] = r

    stats["unique_keys"] = len(latest_by_key)

    trades: list[dict[str, Any]] = []

    for key, r in latest_by_key.items():
        state = str(r.get(state_col) if state_col else "").upper()
        action = str(r.get(action_col) if action_col else "").upper()

        if state in ACTIVE_STATES or action in ACTIVE_ACTIONS:
            stats["skipped_active_or_watch"] += 1
            continue

        entry_price = fnum(r.get(entry_col))
        exit_price = fnum(r.get(exit_col))
        if entry_price is None or entry_price <= 0 or exit_price is None or exit_price <= 0:
            stats["skipped_missing_price"] += 1
            continue

        side = str(r.get(side_col) or "").strip()
        if side not in {"Buy", "Sell"}:
            stats["skipped_unknown_side"] += 1
            continue

        trades.append({
            "source": "executor_trades",
            "key": key,
            "symbol": r.get(symbol_col) if symbol_col else None,
            "side": side,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "state": r.get(state_col) if state_col else None,
            "action": r.get(action_col) if action_col else None,
            "reason": r.get(reason_col) if reason_col else None,
            "created_at": r.get(created_col) if created_col else None,
            "closed_at": r.get(closed_col) if closed_col else None,
        })

    return trades, stats


def calc_trade(t: dict[str, Any], stake: float, leverage: float, fee_bps: float) -> dict[str, Any]:
    entry = float(t["entry_price"])
    exit_ = float(t["exit_price"])
    side = str(t["side"])

    if side == "Sell":
        pct = (entry - exit_) / entry
    else:
        pct = (exit_ - entry) / entry

    notional = stake * leverage
    gross_pnl = notional * pct
    fees = notional * (fee_bps / 10_000.0) * 2.0
    net_pnl = gross_pnl - fees

    out = dict(t)
    out.update({
        "profit_pct": pct * 100.0,
        "stake": stake,
        "leverage": leverage,
        "notional": notional,
        "gross_pnl": gross_pnl,
        "fees": fees,
        "net_pnl": net_pnl,
        "roi_on_stake_pct": (net_pnl / stake * 100.0) if stake else 0.0,
    })
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/signals.db")
    ap.add_argument("--stake", type=float, default=100.0)
    ap.add_argument("--leverage", type=float, default=1.0)
    ap.add_argument("--fee-bps", type=float, default=0.0, help="fee per side in basis points. 6 = 0.06% per side")
    ap.add_argument("--source", choices=["auto", "executor_outcomes", "executor_trades"], default="auto")
    ap.add_argument("--csv", default="")
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        raise SystemExit(f"DB not found: {db}")

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row

    source_used = None
    raw_stats = {}

    if args.source in {"auto", "executor_trades"} and table_exists(conn, "executor_trades"):
        trades, raw_stats = load_executor_trades(conn)
        source_used = "executor_trades"
    elif args.source in {"auto", "executor_outcomes"} and table_exists(conn, "executor_outcomes"):
        trades, raw_stats = load_executor_outcomes(conn)
        source_used = "executor_outcomes"
    else:
        raise SystemExit("No supported table found: executor_outcomes or executor_trades")

    results = [calc_trade(t, args.stake, args.leverage, args.fee_bps) for t in trades]
    results.sort(key=lambda x: str(x.get("closed_at") or ""))

    wins = [r for r in results if r["net_pnl"] > 0]
    losses = [r for r in results if r["net_pnl"] < 0]
    breakeven = [r for r in results if r["net_pnl"] == 0]

    total_gross = sum(r["gross_pnl"] for r in results)
    total_fees = sum(r["fees"] for r in results)
    total_net = sum(r["net_pnl"] for r in results)

    equity = args.stake
    for r in results:
        equity += r["net_pnl"]

    by_side = defaultdict(lambda: {"n": 0, "net": 0.0})
    by_reason = defaultdict(lambda: {"n": 0, "net": 0.0})

    for r in results:
        by_side[r["side"]]["n"] += 1
        by_side[r["side"]]["net"] += r["net_pnl"]

        reason = str(r.get("reason") or "unknown")
        by_reason[reason]["n"] += 1
        by_reason[reason]["net"] += r["net_pnl"]

    print("=== Fixed Stake PnL Calculator ===")
    print(f"DB: {db}")
    print(f"Source: {source_used}")
    print(f"Stake per closed trade: ${args.stake:.2f}")
    print(f"Leverage: {args.leverage:.2f}x")
    print(f"Fee per side: {args.fee_bps:.4f} bps")
    print()
    print("=== Input rows ===")
    for k, v in raw_stats.items():
        print(f"{k}: {v}")
    print()
    print("=== Counted closed trades ===")
    print(f"counted: {len(results)}")
    print(f"wins: {len(wins)}")
    print(f"losses: {len(losses)}")
    print(f"breakeven: {len(breakeven)}")
    winrate = (len(wins) / len(results) * 100.0) if results else 0.0
    print(f"winrate: {winrate:.2f}%")
    print()
    print("=== Money result ===")
    print(f"Total gross PnL: ${total_gross:.2f}")
    print(f"Total fees:      ${total_fees:.2f}")
    print(f"Total net PnL:   ${total_net:.2f}")
    print(f"Final balance if started with $100 and added/subtracted every result: ${equity:.2f}")
    print(f"Average net per trade: ${(total_net / len(results)) if results else 0.0:.4f}")
    print(f"Total deployed notional, counted separately: ${args.stake * args.leverage * len(results):.2f}")
    print()
    print("=== By side ===")
    for side, d in sorted(by_side.items()):
        print(f"{side}: n={d['n']} net=${d['net']:.2f}")
    print()
    print("=== Top exit reasons by net ===")
    for reason, d in sorted(by_reason.items(), key=lambda x: x[1]["net"]):
        print(f"{reason}: n={d['n']} net=${d['net']:.2f}")

    if results:
        print()
        print("=== Biggest losses ===")
        for r in sorted(results, key=lambda x: x["net_pnl"])[:10]:
            print(f"{r.get('closed_at')} {r.get('symbol')} {r.get('side')} {r['profit_pct']:.2f}% net=${r['net_pnl']:.2f} reason={r.get('reason')}")

        print()
        print("=== Biggest wins ===")
        for r in sorted(results, key=lambda x: x["net_pnl"], reverse=True)[:10]:
            print(f"{r.get('closed_at')} {r.get('symbol')} {r.get('side')} {r['profit_pct']:.2f}% net=${r['net_pnl']:.2f} reason={r.get('reason')}")

    if args.csv:
        out = Path(args.csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "source", "key", "symbol", "side", "entry_price", "exit_price",
                "profit_pct", "stake", "leverage", "notional", "gross_pnl",
                "fees", "net_pnl", "roi_on_stake_pct", "state", "action",
                "reason", "created_at", "closed_at",
            ])
            writer.writeheader()
            writer.writerows(results)
        print()
        print(f"CSV saved: {out}")

    conn.close()


if __name__ == "__main__":
    main()
