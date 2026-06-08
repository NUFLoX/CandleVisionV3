from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

CLOSED_STATUSES = {"TP1", "TP2", "SL", "AMBIGUOUS", "EXPIRED"}


def _phase_from_kind(kind: str) -> str:
    k = (kind or "").upper()

    if k == "ACCUMULATION_WATCH":
        return "WATCHING"

    if k == "ABSORPTION_ZONE":
        return "ACCUMULATION"

    if k == "PRE_IMPULSE_ZONE":
        return "PRE_IMPULSE"

    if k == "BREAKOUT_PRESSURE":
        return "BREAKOUT_PRESSURE"

    if k == "SHORT_WATCH":
        return "WATCHING"

    if k == "DISTRIBUTION_ZONE":
        return "DISTRIBUTION"

    if k == "PRE_DUMP_ZONE":
        return "PRE_DUMP"

    if k == "CONFIRMED_BREAKDOWN":
        return "BREAKDOWN_PRESSURE"

    if k == "CONFIRMED_LONG":
        return "CONFIRMED_LONG"

    if k == "CONFIRMED_SHORT":
        return "CONFIRMED_SHORT"

    return "PENDING"


@dataclass(slots=True)
class UpsertResult:
    is_new: bool
    should_notify: bool
    status_changed: bool
    score_jump: bool
    from_status: str | None
    to_status: str
    repeat_count: int


class SignalStore:
    SCHEMA_VERSION = 2

    def __init__(
        self,
        db_path: str = "data/signals.db",
        score_jump_threshold: float = 2.0,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.score_jump_threshold = score_jump_threshold
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cur = self.conn.cursor()

        cur.execute("PRAGMA user_version")
        user_version = int(cur.fetchone()[0])

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                side TEXT NOT NULL,
                score_first REAL NOT NULL,
                score_last REAL NOT NULL,
                score_max REAL NOT NULL,
                entry REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit_1 REAL NOT NULL,
                take_profit_2 REAL NOT NULL,
                reasons_first TEXT NOT NULL,
                reasons_last TEXT NOT NULL,
                meta TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                repeat_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                outcome TEXT,
                outcome_checked_at TEXT,
                time_to_tp1_minutes REAL,
                time_to_tp2_minutes REAL,
                time_to_sl_minutes REAL,
                max_gain_pct REAL,
                max_drawdown_pct REAL
            )
            """
        )

        for col, typ in (
            ("outcome", "TEXT"),
            ("outcome_checked_at", "TEXT"),
            ("time_to_tp1_minutes", "REAL"),
            ("time_to_tp2_minutes", "REAL"),
            ("time_to_sl_minutes", "REAL"),
            ("max_gain_pct", "REAL"),
            ("max_drawdown_pct", "REAL"),
        ):
            try:
                cur.execute(f"ALTER TABLE signals ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_symbol_tf
            ON signals(symbol, timeframe)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signals_status
            ON signals(status)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                event_type TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT,
                score_last REAL,
                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signal_events_key
            ON signal_events(signal_key, created_at)
            """
        )

        self.ensure_executor_schema()
        self.ensure_executor_trade_schema()
        self.ensure_trade_learning_schema()
        self.ensure_trade_diagnosis_schema()
        self.ensure_testnet_order_schema()

        if user_version < self.SCHEMA_VERSION:
            cur.execute(f"PRAGMA user_version = {self.SCHEMA_VERSION}")

        self.conn.commit()


    def ensure_executor_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS executor_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                state TEXT NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL,
                entry_price REAL,
                current_sl REAL,
                exit_price REAL,
                exit_reason TEXT,
                max_gain_r REAL NOT NULL DEFAULT 0,
                max_drawdown_r REAL NOT NULL DEFAULT 0,
                bars_in_trade INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        for col, typ in (
            ("entry_price", "REAL"),
            ("current_sl", "REAL"),
            ("exit_price", "REAL"),
            ("exit_reason", "TEXT"),
            ("max_gain_r", "REAL NOT NULL DEFAULT 0"),
            ("max_drawdown_r", "REAL NOT NULL DEFAULT 0"),
            ("bars_in_trade", "INTEGER NOT NULL DEFAULT 0"),
            ("price", "REAL"),
            ("spread_bps", "REAL"),
            ("buy_flow", "REAL"),
            ("sell_flow", "REAL"),
            ("required_buy_flow", "REAL"),
            ("required_sell_flow", "REAL"),
            ("volume_impulse", "REAL"),
            ("required_volume_impulse", "REAL"),
            ("bid_wall_strength", "REAL"),
            ("ask_wall_strength", "REAL"),
            ("support", "REAL"),
            ("resistance", "REAL"),
            ("ema20", "REAL"),
            ("vwap", "REAL"),
            ("diagnostics_json", "TEXT"),
        ):
            try:
                cur.execute(f"ALTER TABLE executor_outcomes ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_executor_outcomes_symbol
            ON executor_outcomes(symbol, updated_at)
            """
        )
        self.conn.commit()

    def upsert_executor_decision(
        self,
        *,
        signal_key: str,
        symbol: str,
        side: str,
        state: str,
        action: str,
        reason: str,
        entry_price: float | None = None,
        current_sl: float | None = None,
        exit_price: float | None = None,
        exit_reason: str | None = None,
        max_gain_r: float = 0.0,
        max_drawdown_r: float = 0.0,
        bars_in_trade: int = 0,
        price: float | None = None,
        spread_bps: float | None = None,
        buy_flow: float | None = None,
        sell_flow: float | None = None,
        required_buy_flow: float | None = None,
        required_sell_flow: float | None = None,
        volume_impulse: float | None = None,
        required_volume_impulse: float | None = None,
        bid_wall_strength: float | None = None,
        ask_wall_strength: float | None = None,
        support: float | None = None,
        resistance: float | None = None,
        ema20: float | None = None,
        vwap: float | None = None,
        diagnostics_json: str | dict[str, Any] | None = None,
    ) -> sqlite3.Row:
        self.ensure_executor_schema()
        now = _utc_now()
        cur = self.conn.cursor()
        cur.execute("SELECT created_at FROM executor_outcomes WHERE signal_key = ?", (signal_key,))
        existing = cur.fetchone()
        created_at = str(existing["created_at"]) if existing is not None else now
        cur.execute(
            """
            INSERT INTO executor_outcomes (
                signal_key,
                symbol,
                side,
                state,
                action,
                reason,
                entry_price,
                current_sl,
                exit_price,
                exit_reason,
                max_gain_r,
                max_drawdown_r,
                bars_in_trade,
                price,
                spread_bps,
                buy_flow,
                sell_flow,
                required_buy_flow,
                required_sell_flow,
                volume_impulse,
                required_volume_impulse,
                bid_wall_strength,
                ask_wall_strength,
                support,
                resistance,
                ema20,
                vwap,
                diagnostics_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_key) DO UPDATE SET
                symbol=excluded.symbol,
                side=excluded.side,
                state=excluded.state,
                action=excluded.action,
                reason=excluded.reason,
                entry_price=excluded.entry_price,
                current_sl=excluded.current_sl,
                exit_price=excluded.exit_price,
                exit_reason=excluded.exit_reason,
                max_gain_r=excluded.max_gain_r,
                max_drawdown_r=excluded.max_drawdown_r,
                bars_in_trade=excluded.bars_in_trade,
                price=excluded.price,
                spread_bps=excluded.spread_bps,
                buy_flow=excluded.buy_flow,
                sell_flow=excluded.sell_flow,
                required_buy_flow=excluded.required_buy_flow,
                required_sell_flow=excluded.required_sell_flow,
                volume_impulse=excluded.volume_impulse,
                required_volume_impulse=excluded.required_volume_impulse,
                bid_wall_strength=excluded.bid_wall_strength,
                ask_wall_strength=excluded.ask_wall_strength,
                support=excluded.support,
                resistance=excluded.resistance,
                ema20=excluded.ema20,
                vwap=excluded.vwap,
                diagnostics_json=excluded.diagnostics_json,
                updated_at=excluded.updated_at
            """,
            (
                signal_key,
                symbol,
                side,
                state,
                action,
                reason,
                entry_price,
                current_sl,
                exit_price,
                exit_reason,
                float(max_gain_r or 0.0),
                float(max_drawdown_r or 0.0),
                int(bars_in_trade or 0),
                self._optional_float(price),
                self._optional_float(spread_bps),
                self._optional_float(buy_flow),
                self._optional_float(sell_flow),
                self._optional_float(required_buy_flow),
                self._optional_float(required_sell_flow),
                self._optional_float(volume_impulse),
                self._optional_float(required_volume_impulse),
                self._optional_float(bid_wall_strength),
                self._optional_float(ask_wall_strength),
                self._optional_float(support),
                self._optional_float(resistance),
                self._optional_float(ema20),
                self._optional_float(vwap),
                self._json_dumps_safe(diagnostics_json or {}) if not isinstance(diagnostics_json, str) else diagnostics_json,
                created_at,
                now,
            ),
        )
        self.conn.commit()
        row = self.get_executor_outcome(signal_key)
        if row is None:
            raise RuntimeError(f"executor outcome was not stored: {signal_key}")
        return row

    def get_executor_outcome(self, signal_key: str) -> sqlite3.Row | None:
        self.ensure_executor_schema()
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM executor_outcomes WHERE signal_key = ?", (signal_key,))
        return cur.fetchone()

    def list_open_executor_positions(self, limit: int = 100) -> list[sqlite3.Row]:
        self.ensure_executor_schema()
        safe_limit = max(1, int(limit or 100))
        rows = self.conn.execute(
            """
            SELECT *
            FROM executor_outcomes
            WHERE UPPER(COALESCE(state, '')) != 'EXITED'
              AND (
                  UPPER(COALESCE(state, '')) IN ('ENTERED', 'PROTECT_BREAKEVEN', 'TRAILING_PROFIT')
                  OR UPPER(COALESCE(action, '')) = 'HOLD'
              )
            ORDER BY updated_at ASC, id ASC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        return list(rows)


    def ensure_executor_trade_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS executor_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_key TEXT NOT NULL UNIQUE,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT,
                side TEXT NOT NULL,
                state TEXT,
                entry_action TEXT,
                exit_action TEXT,
                entry_price REAL,
                exit_price REAL,
                initial_sl REAL,
                final_sl REAL,
                current_sl REAL,
                entry_time TEXT,
                exit_time TEXT,
                exit_reason TEXT,
                r_result REAL,
                max_gain_r REAL,
                max_drawdown_r REAL,
                bars_in_trade INTEGER,
                duration_minutes REAL,
                moved_to_breakeven INTEGER DEFAULT 0,
                breakeven_time TEXT,
                diagnostics_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        for name, columns in (
            ("idx_executor_trades_signal_key", "signal_key"),
            ("idx_executor_trades_symbol_timeframe", "symbol, timeframe"),
            ("idx_executor_trades_exit_reason", "exit_reason"),
            ("idx_executor_trades_exit_time", "exit_time"),
            ("idx_executor_trades_r_result", "r_result"),
        ):
            cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON executor_trades({columns})")
        self.conn.commit()

    def upsert_executor_trade(self, trade: dict[str, Any]) -> None:
        self.ensure_executor_trade_schema()
        now = _utc_now()
        trade_key = str(trade.get("trade_key") or "").strip()
        signal_key = str(trade.get("signal_key") or "").strip()
        symbol = str(trade.get("symbol") or "").strip()
        side = str(trade.get("side") or "").strip()
        if not trade_key or not signal_key or not symbol or not side:
            raise ValueError("executor trade requires trade_key, signal_key, symbol, and side")

        existing = self.conn.execute(
            "SELECT created_at FROM executor_trades WHERE trade_key = ?",
            (trade_key,),
        ).fetchone()
        created_at = str(existing["created_at"]) if existing is not None else str(trade.get("created_at") or now)
        updated_at = str(trade.get("updated_at") or now)
        diagnostics_json = trade.get("diagnostics_json")
        if diagnostics_json is not None and not isinstance(diagnostics_json, str):
            diagnostics_json = self._json_dumps_safe(diagnostics_json)

        self.conn.execute(
            """
            INSERT INTO executor_trades (
                trade_key,
                signal_key,
                symbol,
                timeframe,
                side,
                state,
                entry_action,
                exit_action,
                entry_price,
                exit_price,
                initial_sl,
                final_sl,
                current_sl,
                entry_time,
                exit_time,
                exit_reason,
                r_result,
                max_gain_r,
                max_drawdown_r,
                bars_in_trade,
                duration_minutes,
                moved_to_breakeven,
                breakeven_time,
                diagnostics_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_key) DO UPDATE SET
                signal_key=excluded.signal_key,
                symbol=excluded.symbol,
                timeframe=excluded.timeframe,
                side=excluded.side,
                state=excluded.state,
                entry_action=excluded.entry_action,
                exit_action=excluded.exit_action,
                entry_price=excluded.entry_price,
                exit_price=excluded.exit_price,
                initial_sl=excluded.initial_sl,
                final_sl=excluded.final_sl,
                current_sl=excluded.current_sl,
                entry_time=excluded.entry_time,
                exit_time=excluded.exit_time,
                exit_reason=excluded.exit_reason,
                r_result=excluded.r_result,
                max_gain_r=excluded.max_gain_r,
                max_drawdown_r=excluded.max_drawdown_r,
                bars_in_trade=excluded.bars_in_trade,
                duration_minutes=excluded.duration_minutes,
                moved_to_breakeven=excluded.moved_to_breakeven,
                breakeven_time=excluded.breakeven_time,
                diagnostics_json=excluded.diagnostics_json,
                updated_at=excluded.updated_at
            """,
            (
                trade_key,
                signal_key,
                symbol,
                self._optional_text(trade.get("timeframe")),
                side,
                self._optional_text(trade.get("state")),
                self._optional_text(trade.get("entry_action")),
                self._optional_text(trade.get("exit_action")),
                self._optional_float(trade.get("entry_price")),
                self._optional_float(trade.get("exit_price")),
                self._optional_float(trade.get("initial_sl")),
                self._optional_float(trade.get("final_sl")),
                self._optional_float(trade.get("current_sl")),
                self._optional_text(trade.get("entry_time")),
                self._optional_text(trade.get("exit_time")),
                self._optional_text(trade.get("exit_reason")),
                self._optional_float(trade.get("r_result")),
                self._optional_float(trade.get("max_gain_r")),
                self._optional_float(trade.get("max_drawdown_r")),
                int(trade["bars_in_trade"]) if trade.get("bars_in_trade") not in (None, "") else None,
                self._optional_float(trade.get("duration_minutes")),
                1 if trade.get("moved_to_breakeven") else 0,
                self._optional_text(trade.get("breakeven_time")),
                diagnostics_json,
                created_at,
                updated_at,
            ),
        )
        self.conn.commit()

    def get_executor_trade(self, trade_key: str) -> dict[str, Any] | None:
        self.ensure_executor_trade_schema()
        row = self.conn.execute("SELECT * FROM executor_trades WHERE trade_key = ?", (trade_key,)).fetchone()
        return dict(row) if row is not None else None

    def get_latest_executor_trade_for_signal(self, signal_key: str) -> dict[str, Any] | None:
        self.ensure_executor_trade_schema()
        row = self.conn.execute(
            """
            SELECT *
            FROM executor_trades
            WHERE signal_key = ?
            ORDER BY exit_time DESC, updated_at DESC, id DESC
            LIMIT 1
            """,
            (signal_key,),
        ).fetchone()
        return dict(row) if row is not None else None

    def list_executor_trades(self, limit: int = 100, symbol: str | None = None) -> list[dict[str, Any]]:
        self.ensure_executor_trade_schema()
        safe_limit = max(1, int(limit or 100))
        if symbol:
            rows = self.conn.execute(
                "SELECT * FROM executor_trades WHERE symbol = ? ORDER BY exit_time DESC, id DESC LIMIT ?",
                (symbol, safe_limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM executor_trades ORDER BY exit_time DESC, id DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def ensure_trade_learning_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_lifecycle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT,
                side TEXT,
                event_type TEXT NOT NULL,
                status TEXT,
                action TEXT,
                reason TEXT,
                price REAL,
                score REAL,
                btc_regime TEXT,
                market_regime TEXT,
                features_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_signal_key
            ON trade_lifecycle_events(signal_key)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_symbol_timeframe
            ON trade_lifecycle_events(symbol, timeframe)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_event_type
            ON trade_lifecycle_events(event_type)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_lifecycle_created_at
            ON trade_lifecycle_events(created_at)
            """
        )
        self.conn.commit()


    def ensure_testnet_order_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS testnet_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL,
                trade_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                category TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                qty REAL,
                notional_usdt REAL,
                price REAL,
                order_id TEXT,
                order_link_id TEXT,
                status TEXT NOT NULL,
                reason TEXT,
                request_json TEXT,
                response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_testnet_orders_signal_key ON testnet_orders(signal_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_testnet_orders_trade_key ON testnet_orders(trade_key)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_testnet_orders_status ON testnet_orders(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_testnet_orders_created_at ON testnet_orders(created_at)")
        self.conn.commit()

    def insert_testnet_order(self, order: dict[str, Any]) -> dict[str, Any]:
        self.ensure_testnet_order_schema()
        now = _utc_now()
        created_at = str(order.get("created_at") or now)
        updated_at = str(order.get("updated_at") or now)
        request_json = order.get("request_json")
        response_json = order.get("response_json")
        if not isinstance(request_json, str):
            request_json = self._json_dumps_safe(request_json or {})
        if not isinstance(response_json, str):
            response_json = self._json_dumps_safe(response_json or {})
        cur = self.conn.execute(
            """
            INSERT INTO testnet_orders (
                signal_key, trade_key, symbol, category, side, order_type, qty, notional_usdt, price,
                order_id, order_link_id, status, reason, request_json, response_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(order.get("signal_key") or ""),
                str(order.get("trade_key") or ""),
                str(order.get("symbol") or ""),
                str(order.get("category") or "linear"),
                str(order.get("side") or ""),
                str(order.get("order_type") or "Market"),
                self._optional_float(order.get("qty")),
                self._optional_float(order.get("notional_usdt")),
                self._optional_float(order.get("price")),
                self._optional_text(order.get("order_id")),
                self._optional_text(order.get("order_link_id")),
                str(order.get("status") or "blocked"),
                self._optional_text(order.get("reason")),
                request_json,
                response_json,
                created_at,
                updated_at,
            ),
        )
        self.conn.commit()
        row = self.conn.execute("SELECT * FROM testnet_orders WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row) if row is not None else {}

    def get_testnet_order_by_signal(self, signal_key: str) -> dict[str, Any] | None:
        self.ensure_testnet_order_schema()
        row = self.conn.execute(
            "SELECT * FROM testnet_orders WHERE signal_key = ? AND status IN ('placed', 'filled') ORDER BY id DESC LIMIT 1",
            (signal_key,),
        ).fetchone()
        return dict(row) if row is not None else None

    def get_latest_open_testnet_order(self, signal_key: str) -> dict[str, Any] | None:
        self.ensure_testnet_order_schema()
        row = self.conn.execute(
            """
            SELECT * FROM testnet_orders
            WHERE signal_key = ? AND side = 'Buy' AND status IN ('placed', 'filled')
              AND NOT EXISTS (
                  SELECT 1 FROM testnet_orders closes
                  WHERE closes.signal_key = testnet_orders.signal_key
                    AND closes.side = 'Sell'
                    AND closes.status IN ('placed', 'filled')
                    AND closes.created_at >= testnet_orders.created_at
              )
            ORDER BY id DESC LIMIT 1
            """,
            (signal_key,),
        ).fetchone()
        return dict(row) if row is not None else None

    def count_open_testnet_positions(self) -> int:
        self.ensure_testnet_order_schema()
        row = self.conn.execute(
            """
            SELECT COUNT(*) FROM testnet_orders entries
            WHERE entries.side = 'Buy' AND entries.status IN ('placed', 'filled')
              AND NOT EXISTS (
                  SELECT 1 FROM testnet_orders closes
                  WHERE closes.signal_key = entries.signal_key
                    AND closes.side = 'Sell'
                    AND closes.status IN ('placed', 'filled')
                    AND closes.created_at >= entries.created_at
              )
            """
        ).fetchone()
        return int(row[0] or 0)

    def count_testnet_orders_since(self, iso_since: str) -> int:
        self.ensure_testnet_order_schema()
        row = self.conn.execute(
            "SELECT COUNT(*) FROM testnet_orders WHERE status IN ('placed', 'filled') AND created_at >= ?",
            (iso_since,),
        ).fetchone()
        return int(row[0] or 0)

    def list_testnet_orders(self, limit: int = 100) -> list[dict[str, Any]]:
        self.ensure_testnet_order_schema()
        rows = self.conn.execute(
            "SELECT * FROM testnet_orders ORDER BY created_at DESC, id DESC LIMIT ?",
            (max(1, int(limit or 100)),),
        ).fetchall()
        return [dict(row) for row in rows]

    def ensure_trade_diagnosis_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_diagnoses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                timeframe TEXT,
                side TEXT,
                outcome TEXT NOT NULL,
                diagnosis TEXT NOT NULL,
                success_factors_json TEXT,
                failure_factors_json TEXT,
                recommendation TEXT,
                r_result REAL,
                max_gain_pct REAL,
                max_drawdown_pct REAL,
                time_to_tp1_minutes REAL,
                time_to_tp2_minutes REAL,
                time_to_sl_minutes REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_diagnoses_symbol_timeframe
            ON trade_diagnoses(symbol, timeframe)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_diagnoses_outcome
            ON trade_diagnoses(outcome)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_trade_diagnoses_created_at
            ON trade_diagnoses(created_at)
            """
        )
        self.conn.commit()

    def upsert_trade_diagnosis(self, diagnosis: dict[str, Any]) -> None:
        self.ensure_trade_diagnosis_schema()
        now = _utc_now()
        signal_key = str(diagnosis.get("signal_key") or "")
        existing = self.conn.execute(
            "SELECT created_at FROM trade_diagnoses WHERE signal_key = ?",
            (signal_key,),
        ).fetchone()
        created_at = str(existing["created_at"]) if existing is not None else str(diagnosis.get("created_at") or now)
        updated_at = str(diagnosis.get("updated_at") or now)

        self.conn.execute(
            """
            INSERT INTO trade_diagnoses (
                signal_key,
                symbol,
                timeframe,
                side,
                outcome,
                diagnosis,
                success_factors_json,
                failure_factors_json,
                recommendation,
                r_result,
                max_gain_pct,
                max_drawdown_pct,
                time_to_tp1_minutes,
                time_to_tp2_minutes,
                time_to_sl_minutes,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_key) DO UPDATE SET
                symbol=excluded.symbol,
                timeframe=excluded.timeframe,
                side=excluded.side,
                outcome=excluded.outcome,
                diagnosis=excluded.diagnosis,
                success_factors_json=excluded.success_factors_json,
                failure_factors_json=excluded.failure_factors_json,
                recommendation=excluded.recommendation,
                r_result=excluded.r_result,
                max_gain_pct=excluded.max_gain_pct,
                max_drawdown_pct=excluded.max_drawdown_pct,
                time_to_tp1_minutes=excluded.time_to_tp1_minutes,
                time_to_tp2_minutes=excluded.time_to_tp2_minutes,
                time_to_sl_minutes=excluded.time_to_sl_minutes,
                updated_at=excluded.updated_at
            """,
            (
                signal_key,
                str(diagnosis.get("symbol") or ""),
                self._optional_text(diagnosis.get("timeframe")),
                self._optional_text(diagnosis.get("side")),
                str(diagnosis.get("outcome") or ""),
                str(diagnosis.get("diagnosis") or ""),
                self._json_dumps_safe(diagnosis.get("success_factors") or {}),
                self._json_dumps_safe(diagnosis.get("failure_factors") or {}),
                self._optional_text(diagnosis.get("recommendation")),
                self._optional_float(diagnosis.get("r_result")),
                self._optional_float(diagnosis.get("max_gain_pct")),
                self._optional_float(diagnosis.get("max_drawdown_pct")),
                self._optional_float(diagnosis.get("time_to_tp1_minutes")),
                self._optional_float(diagnosis.get("time_to_tp2_minutes")),
                self._optional_float(diagnosis.get("time_to_sl_minutes")),
                created_at,
                updated_at,
            ),
        )
        self.conn.commit()

    def get_trade_diagnosis(self, signal_key: str) -> dict[str, Any] | None:
        self.ensure_trade_diagnosis_schema()
        row = self.conn.execute(
            "SELECT * FROM trade_diagnoses WHERE signal_key = ?",
            (signal_key,),
        ).fetchone()
        if row is None:
            return None

        item = dict(row)
        for json_col, output_col in (
            ("success_factors_json", "success_factors"),
            ("failure_factors_json", "failure_factors"),
        ):
            raw = item.pop(json_col, None)
            try:
                item[output_col] = json.loads(raw or "{}")
            except json.JSONDecodeError:
                item[output_col] = {}
        return item

    def has_trade_lifecycle_event(self, signal_key: str, event_type: str) -> bool:
        self.ensure_trade_learning_schema()
        row = self.conn.execute(
            """
            SELECT 1 FROM trade_lifecycle_events
            WHERE signal_key = ? AND event_type = ?
            LIMIT 1
            """,
            (signal_key, event_type),
        ).fetchone()
        return row is not None

    def add_trade_lifecycle_event(self, event: Any) -> None:
        self.ensure_trade_learning_schema()
        payload = self._trade_lifecycle_payload(event)
        features_json = self._json_dumps_safe(payload.get("features") or {})
        created_at = str(payload.get("created_at") or _utc_now())

        self.conn.execute(
            """
            INSERT INTO trade_lifecycle_events (
                signal_key,
                symbol,
                timeframe,
                side,
                event_type,
                status,
                action,
                reason,
                price,
                score,
                btc_regime,
                market_regime,
                features_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload.get("signal_key") or ""),
                str(payload.get("symbol") or ""),
                self._optional_text(payload.get("timeframe")),
                self._optional_text(payload.get("side")),
                str(payload.get("event_type") or ""),
                self._optional_text(payload.get("status")),
                self._optional_text(payload.get("action")),
                self._optional_text(payload.get("reason")),
                self._optional_float(payload.get("price")),
                self._optional_float(payload.get("score")),
                self._optional_text(payload.get("btc_regime")),
                self._optional_text(payload.get("market_regime")),
                features_json,
                created_at,
            ),
        )
        self.conn.commit()

    def get_trade_lifecycle_events(self, signal_key: str) -> list[dict[str, Any]]:
        self.ensure_trade_learning_schema()
        rows = self.conn.execute(
            """
            SELECT * FROM trade_lifecycle_events
            WHERE signal_key = ?
            ORDER BY created_at ASC, id ASC
            """,
            (signal_key,),
        ).fetchall()

        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            features_raw = item.pop("features_json", None)
            try:
                item["features"] = json.loads(features_raw or "{}")
            except json.JSONDecodeError:
                item["features"] = {}
            events.append(item)
        return events

    @staticmethod
    def _trade_lifecycle_payload(event: Any) -> dict[str, Any]:
        if isinstance(event, dict):
            return dict(event)
        if is_dataclass(event):
            return asdict(event)
        return dict(getattr(event, "__dict__", {}) or {})

    @classmethod
    def _json_dumps_safe(cls, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            return json.dumps(cls._stringify_json_value(value), ensure_ascii=False)

    @classmethod
    def _stringify_json_value(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): cls._stringify_json_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [cls._stringify_json_value(v) for v in value]
        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return str(value)

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _optional_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    def add_event(
        self,
        *,
        signal_key: str,
        symbol: str,
        timeframe: str,
        event_type: str,
        from_status: str | None,
        to_status: str | None,
        score_last: float | None = None,
    ) -> None:
        cur = self.conn.cursor()

        cur.execute(
            """
            INSERT INTO signal_events (
                signal_key,
                symbol,
                timeframe,
                event_type,
                from_status,
                to_status,
                score_last,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_key,
                symbol,
                timeframe,
                event_type,
                from_status,
                to_status,
                score_last,
                _utc_now(),
            ),
        )

        self.conn.commit()

    def upsert_signal(self, signal: Any, *, market: str = "linear") -> UpsertResult:
        symbol = str(getattr(signal, "symbol", "UNKNOWN"))
        side = str(getattr(signal, "side", "Buy"))
        kind = str(getattr(signal, "kind", "SIGNAL"))
        source = str(getattr(signal, "source", "orderflow"))

        score = float(getattr(signal, "score", 0.0) or 0.0)
        entry = float(getattr(signal, "entry", 0.0) or 0.0)
        sl = float(getattr(signal, "stop_loss", 0.0) or 0.0)
        tp1 = float(getattr(signal, "take_profit_1", 0.0) or 0.0)
        tp2 = float(getattr(signal, "take_profit_2", 0.0) or 0.0)

        reasons = list(getattr(signal, "reasons", []) or [])
        meta = dict(getattr(signal, "meta", {}) or {})

        timeframe = str(meta.get("tf") or "1")
        status = _phase_from_kind(kind)
        signal_key = f"{symbol}|{market}|{timeframe}|{kind}|{side}"
        now = _utc_now()

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM signals WHERE signal_key = ?", (signal_key,))
        row = cur.fetchone()

        if row is None:
            cur.execute(
                """
                INSERT INTO signals (
                    signal_key,
                    symbol,
                    market,
                    timeframe,
                    source,
                    kind,
                    side,
                    score_first,
                    score_last,
                    score_max,
                    entry,
                    stop_loss,
                    take_profit_1,
                    take_profit_2,
                    reasons_first,
                    reasons_last,
                    meta,
                    first_seen,
                    last_seen,
                    repeat_count,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_key,
                    symbol,
                    market,
                    timeframe,
                    source,
                    kind,
                    side,
                    score,
                    score,
                    score,
                    entry,
                    sl,
                    tp1,
                    tp2,
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(reasons, ensure_ascii=False),
                    json.dumps(meta, ensure_ascii=False),
                    now,
                    now,
                    1,
                    status,
                ),
            )

            self.conn.commit()

            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="new_setup",
                from_status=None,
                to_status=status,
                score_last=score,
            )

            return UpsertResult(
                is_new=True,
                should_notify=True,
                status_changed=False,
                score_jump=False,
                from_status=None,
                to_status=status,
                repeat_count=1,
            )

        prev_status = str(row["status"] or "PENDING")
        prev_status_u = prev_status.upper()
        prev_outcome = str(row["outcome"] or "").upper()
        prev_score = float(row["score_last"])
        prev_max = float(row["score_max"])
        repeat_count = int(row["repeat_count"]) + 1

        is_closed = prev_status_u in CLOSED_STATUSES or prev_outcome in CLOSED_STATUSES

        if is_closed:
            status = prev_outcome if prev_outcome in CLOSED_STATUSES else prev_status_u
            score_jump = False
            status_changed = False
            should_notify = False
        else:
            score_jump = (score - prev_score) >= self.score_jump_threshold
            status_changed = status != prev_status
            should_notify = status_changed or score_jump

        cur.execute(
            """
            UPDATE signals
            SET
                score_last = ?,
                score_max = ?,
                reasons_last = ?,
                meta = ?,
                last_seen = ?,
                repeat_count = ?,
                status = ?,
                entry = ?,
                stop_loss = ?,
                take_profit_1 = ?,
                take_profit_2 = ?
            WHERE signal_key = ?
            """,
            (
                score,
                max(prev_max, score),
                json.dumps(reasons, ensure_ascii=False),
                json.dumps(meta, ensure_ascii=False),
                now,
                repeat_count,
                status,
                entry,
                sl,
                tp1,
                tp2,
                signal_key,
            ),
        )

        self.conn.commit()

        if is_closed:
            return UpsertResult(
                is_new=False,
                should_notify=False,
                status_changed=False,
                score_jump=False,
                from_status=prev_status,
                to_status=status,
                repeat_count=repeat_count,
            )

        if status_changed:
            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="status_changed",
                from_status=prev_status,
                to_status=status,
                score_last=score,
            )

        if score_jump:
            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="score_jump",
                from_status=prev_status,
                to_status=status,
                score_last=score,
            )

        if not status_changed and not score_jump:
            self.add_event(
                signal_key=signal_key,
                symbol=symbol,
                timeframe=timeframe,
                event_type="repeat",
                from_status=prev_status,
                to_status=status,
                score_last=score,
            )

        return UpsertResult(
            is_new=False,
            should_notify=should_notify,
            status_changed=status_changed,
            score_jump=score_jump,
            from_status=prev_status,
            to_status=status,
            repeat_count=repeat_count,
        )

    def promote_signal(
        self,
        *,
        signal_key: str,
        to_status: str,
        score_last: float | None = None,
    ) -> bool:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT symbol, timeframe, status, score_last
            FROM signals
            WHERE signal_key = ?
            """,
            (signal_key,),
        )
        row = cur.fetchone()

        if row is None:
            return False

        from_status = str(row["status"] or "PENDING")

        if from_status == to_status:
            return False

        score = float(score_last if score_last is not None else row["score_last"] or 0.0)
        now = _utc_now()

        cur.execute(
            """
            UPDATE signals
            SET
                status = ?,
                score_last = ?,
                last_seen = ?
            WHERE signal_key = ?
            """,
            (
                to_status,
                score,
                now,
                signal_key,
            ),
        )

        self.conn.commit()

        self.add_event(
            signal_key=signal_key,
            symbol=str(row["symbol"]),
            timeframe=str(row["timeframe"]),
            event_type="promoted_to_confirmed",
            from_status=from_status,
            to_status=to_status,
            score_last=score,
        )

        return True

    def close(self) -> None:
        self.conn.close()
