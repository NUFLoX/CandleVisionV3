from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


_SECRET_MARKERS = (
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "API_KEY",
    "PRIVATE",
    "CHAT_ID",
)

_CONFIG_PREFIXES = (
    "ACC_",
    "EXECUTOR_",
    "HYBRID_ENTRY_SHADOW_",
    "SIGNAL_FORWARD_",
    "RUN_TRADE_EXECUTOR",
    "TRADE_EXECUTOR_MODE",
)

_CONFIG_EXCLUDED = {
    "RESEARCH_RUN_ENABLED",
    "RESEARCH_RUN_ID",
    "RESEARCH_RUN_LABEL",
    "RESEARCH_RUN_STRATEGY_ID",
    "RESEARCH_RUN_STRATEGY_VERSION",
    "RESEARCH_CODE_SHA",
    "RESEARCH_DASHBOARD_DEFAULT",
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()

def _parse_utc(value: object | None) -> datetime | None:
    normalized = _text(value)
    if normalized is None:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC)


def _text(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _float_or_none(value: object | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: object | None) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_text(value: object | None) -> str:
    if value is None:
        return "{}"

    if isinstance(value, str):
        return value

    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return "{}"


def research_runs_enabled() -> bool:
    return os.getenv(
        "RESEARCH_RUN_ENABLED",
        "false",
    ).strip().lower() in {"1", "true", "yes", "on"}


def _config_snapshot() -> dict[str, str]:
    snapshot: dict[str, str] = {}

    for name, value in os.environ.items():
        upper_name = name.upper()

        if upper_name in _CONFIG_EXCLUDED:
            continue

        if any(marker in upper_name for marker in _SECRET_MARKERS):
            continue

        if not any(
            upper_name.startswith(prefix)
            for prefix in _CONFIG_PREFIXES
        ):
            continue

        snapshot[upper_name] = str(value)

    return dict(sorted(snapshot.items()))


def _resolve_code_sha() -> str:
    configured = _text(os.getenv("RESEARCH_CODE_SHA"))
    if configured:
        return configured

    root = Path(__file__).resolve().parents[1]

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"

    return _text(result.stdout) or "unknown"


def _run_id_fragment(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in {"-", "_"} else "-"
        for char in value
    ).strip("-") or "research"


@dataclass(frozen=True, slots=True)
class ResearchRunContext:
    strategy_id: str
    strategy_version: str
    mode: str
    code_sha: str
    config_hash: str
    config_json: str
    label: str | None = None
    requested_run_id: str | None = None

    @classmethod
    def from_env(cls) -> "ResearchRunContext":
        snapshot = _config_snapshot()
        config_json = json.dumps(
            snapshot,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        config_hash = hashlib.sha256(
            config_json.encode("utf-8")
        ).hexdigest()

        return cls(
            strategy_id=(
                _text(os.getenv("RESEARCH_RUN_STRATEGY_ID"))
                or "accumulation_executor_baseline"
            ),
            strategy_version=(
                _text(os.getenv("RESEARCH_RUN_STRATEGY_VERSION"))
                or "v1"
            ),
            mode=(
                _text(os.getenv("TRADE_EXECUTOR_MODE"))
                or "paper"
            ).lower(),
            code_sha=_resolve_code_sha(),
            config_hash=config_hash,
            config_json=config_json,
            label=_text(os.getenv("RESEARCH_RUN_LABEL")),
            requested_run_id=_text(os.getenv("RESEARCH_RUN_ID")),
        )


class ResearchRunLedger:
    """
    Isolated research storage.

    This class never alters executor_outcomes or executor_trades.
    All research data lives only in research_* tables.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        enabled: bool,
        context: ResearchRunContext | None = None,
    ) -> None:
        self.conn = conn
        self.enabled = bool(enabled)
        self.context = context
        self.run_id: str | None = None
        self.started_at: str | None = None

        if not self.enabled:
            return

        self.context = context or ResearchRunContext.from_env()
        self.ensure_schema()
        self.run_id = self._get_or_create_run()
        self.started_at = self._load_started_at()

    def ensure_schema(self) -> None:
        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_runs (
                run_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                strategy_version TEXT NOT NULL,
                mode TEXT NOT NULL,
                code_sha TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                config_json TEXT NOT NULL,
                label TEXT,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                ended_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_runs_active
            ON research_runs(status, started_at DESC)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_executor_observations (
                run_id TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT,
                side TEXT,
                state TEXT,
                action TEXT,
                reason TEXT,
                entry_price REAL,
                current_sl REAL,
                exit_price REAL,
                exit_reason TEXT,
                max_gain_r REAL,
                max_drawdown_r REAL,
                bars_in_trade INTEGER,
                signal_kind TEXT,
                btc_regime TEXT,
                market_regime TEXT,
                diagnostics_json TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                observation_count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (run_id, signal_key),
                FOREIGN KEY (run_id) REFERENCES research_runs(run_id)
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_observations_run_state
            ON research_executor_observations(
                run_id,
                state,
                action,
                last_seen_at DESC
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS research_executor_trades (
                run_id TEXT NOT NULL,
                trade_key TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT,
                side TEXT,
                state TEXT,
                entry_price REAL,
                exit_price REAL,
                initial_sl REAL,
                final_sl REAL,
                exit_reason TEXT,
                r_result REAL,
                max_gain_r REAL,
                max_drawdown_r REAL,
                bars_in_trade INTEGER,
                duration_minutes REAL,
                moved_to_breakeven INTEGER NOT NULL DEFAULT 0,
                entry_time TEXT,
                exit_time TEXT,
                signal_kind TEXT,
                btc_regime TEXT,
                market_regime TEXT,
                diagnostics_json TEXT NOT NULL,
                first_recorded_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, trade_key),
                FOREIGN KEY (run_id) REFERENCES research_runs(run_id)
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_trades_run_exit
            ON research_executor_trades(
                run_id,
                exit_time DESC
            )
            """
        )

        self.conn.commit()

    def _get_or_create_run(self) -> str:
        assert self.context is not None

        now = _utc_now()
        context = self.context

        if context.requested_run_id:
            existing = self.conn.execute(
                """
                SELECT run_id
                FROM research_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (context.requested_run_id,),
            ).fetchone()

            if existing is not None:
                self.conn.execute(
                    """
                    UPDATE research_runs
                    SET last_seen_at = ?, status = 'ACTIVE', ended_at = NULL
                    WHERE run_id = ?
                    """,
                    (now, context.requested_run_id),
                )
                self.conn.commit()
                return context.requested_run_id

            self._insert_run(
                run_id=context.requested_run_id,
                status="ACTIVE",
                now=now,
            )
            return context.requested_run_id

        existing = self.conn.execute(
            """
            SELECT run_id
            FROM research_runs
            WHERE strategy_id = ?
              AND strategy_version = ?
              AND mode = ?
              AND code_sha = ?
              AND config_hash = ?
              AND status = 'ACTIVE'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (
                context.strategy_id,
                context.strategy_version,
                context.mode,
                context.code_sha,
                context.config_hash,
            ),
        ).fetchone()

        if existing is not None:
            run_id = str(existing["run_id"])
            self.conn.execute(
                """
                UPDATE research_runs
                SET last_seen_at = ?
                WHERE run_id = ?
                """,
                (now, run_id),
            )
            self.conn.commit()
            return run_id

        self.conn.execute(
            """
            UPDATE research_runs
            SET status = 'SUPERSEDED',
                ended_at = ?,
                last_seen_at = ?
            WHERE strategy_id = ?
              AND strategy_version = ?
              AND mode = ?
              AND status = 'ACTIVE'
            """,
            (
                now,
                now,
                context.strategy_id,
                context.strategy_version,
                context.mode,
            ),
        )

        run_id = "-".join(
            (
                _run_id_fragment(context.strategy_id),
                _run_id_fragment(context.strategy_version),
                datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"),
                context.config_hash[:10],
            )
        )

        self._insert_run(
            run_id=run_id,
            status="ACTIVE",
            now=now,
        )
        return run_id

    def _insert_run(
        self,
        *,
        run_id: str,
        status: str,
        now: str,
    ) -> None:
        assert self.context is not None

        self.conn.execute(
            """
            INSERT INTO research_runs (
                run_id,
                strategy_id,
                strategy_version,
                mode,
                code_sha,
                config_hash,
                config_json,
                label,
                status,
                started_at,
                last_seen_at,
                ended_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                run_id,
                self.context.strategy_id,
                self.context.strategy_version,
                self.context.mode,
                self.context.code_sha,
                self.context.config_hash,
                self.context.config_json,
                self.context.label,
                status,
                now,
                now,
            ),
        )
        self.conn.commit()

    def _touch_run(self) -> None:
        if not self.enabled or not self.run_id:
            return

        self.conn.execute(
            """
            UPDATE research_runs
            SET last_seen_at = ?
            WHERE run_id = ?
            """,
            (_utc_now(), self.run_id),
        )

    def _load_started_at(self) -> str | None:
        if not self.run_id:
            return None

        row = self.conn.execute(
            "SELECT started_at FROM research_runs WHERE run_id = ?",
            (self.run_id,),
        ).fetchone()

        if row is None:
            return None

        return _text(row["started_at"])

    def accepts_source_created_at(
        self,
        source_created_at: object | None,
    ) -> bool:
        if not self.enabled or not self.run_id:
            return False

        source_dt = _parse_utc(source_created_at)
        started_dt = _parse_utc(self.started_at)

        if source_dt is None or started_dt is None:
            return False

        return source_dt >= started_dt

    def metadata(self) -> dict[str, str | None]:
        if not self.enabled or not self.context or not self.run_id:
            return {}

        return {
            "research_run_id": self.run_id,
            "research_strategy_id": self.context.strategy_id,
            "research_strategy_version": self.context.strategy_version,
            "research_mode": self.context.mode,
            "research_code_sha": self.context.code_sha,
            "research_config_hash": self.context.config_hash,
            "research_label": self.context.label,
        }

    def record_observation(
        self,
        *,
        signal_key: str,
        symbol: str,
        timeframe: str | None,
        side: str | None,
        state: str | None,
        action: str | None,
        reason: str | None,
        entry_price: object | None = None,
        current_sl: object | None = None,
        exit_price: object | None = None,
        exit_reason: str | None = None,
        max_gain_r: object | None = None,
        max_drawdown_r: object | None = None,
        bars_in_trade: object | None = None,
        signal_kind: str | None = None,
        btc_regime: str | None = None,
        market_regime: str | None = None,
        diagnostics: object | None = None,
    ) -> None:
        if not self.enabled or not self.run_id:
            return

        normalized_key = _text(signal_key)
        normalized_symbol = _text(symbol)

        if not normalized_key or not normalized_symbol:
            raise ValueError(
                "research observation requires signal_key and symbol"
            )

        now = _utc_now()

        self.conn.execute(
            """
            INSERT INTO research_executor_observations (
                run_id,
                signal_key,
                symbol,
                timeframe,
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
                signal_kind,
                btc_regime,
                market_regime,
                diagnostics_json,
                first_seen_at,
                last_seen_at,
                observation_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(run_id, signal_key) DO UPDATE SET
                symbol = excluded.symbol,
                timeframe = excluded.timeframe,
                side = excluded.side,
                state = excluded.state,
                action = excluded.action,
                reason = excluded.reason,
                entry_price = excluded.entry_price,
                current_sl = excluded.current_sl,
                exit_price = excluded.exit_price,
                exit_reason = excluded.exit_reason,
                max_gain_r = excluded.max_gain_r,
                max_drawdown_r = excluded.max_drawdown_r,
                bars_in_trade = excluded.bars_in_trade,
                signal_kind = excluded.signal_kind,
                btc_regime = excluded.btc_regime,
                market_regime = excluded.market_regime,
                diagnostics_json = excluded.diagnostics_json,
                last_seen_at = excluded.last_seen_at,
                observation_count = (
                    research_executor_observations.observation_count + 1
                )
            """,
            (
                self.run_id,
                normalized_key,
                normalized_symbol,
                _text(timeframe),
                _text(side),
                _text(state),
                _text(action),
                _text(reason),
                _float_or_none(entry_price),
                _float_or_none(current_sl),
                _float_or_none(exit_price),
                _text(exit_reason),
                _float_or_none(max_gain_r),
                _float_or_none(max_drawdown_r),
                _int_or_none(bars_in_trade),
                _text(signal_kind),
                _text(btc_regime),
                _text(market_regime),
                _json_text(diagnostics),
                now,
                now,
            ),
        )

        self._touch_run()
        self.conn.commit()

    def record_trade(
        self,
        trade: dict[str, Any],
        *,
        signal_kind: str | None = None,
        btc_regime: str | None = None,
        market_regime: str | None = None,
    ) -> None:
        if not self.enabled or not self.run_id:
            return

        trade_key = _text(trade.get("trade_key"))
        signal_key = _text(trade.get("signal_key"))
        symbol = _text(trade.get("symbol"))

        if not trade_key or not signal_key or not symbol:
            raise ValueError(
                "research trade requires trade_key, signal_key and symbol"
            )

        now = _utc_now()

        self.conn.execute(
            """
            INSERT INTO research_executor_trades (
                run_id,
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
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, trade_key) DO UPDATE SET
                signal_key = excluded.signal_key,
                symbol = excluded.symbol,
                timeframe = excluded.timeframe,
                side = excluded.side,
                state = excluded.state,
                entry_price = excluded.entry_price,
                exit_price = excluded.exit_price,
                initial_sl = excluded.initial_sl,
                final_sl = excluded.final_sl,
                exit_reason = excluded.exit_reason,
                r_result = excluded.r_result,
                max_gain_r = excluded.max_gain_r,
                max_drawdown_r = excluded.max_drawdown_r,
                bars_in_trade = excluded.bars_in_trade,
                duration_minutes = excluded.duration_minutes,
                moved_to_breakeven = excluded.moved_to_breakeven,
                entry_time = excluded.entry_time,
                exit_time = excluded.exit_time,
                signal_kind = excluded.signal_kind,
                btc_regime = excluded.btc_regime,
                market_regime = excluded.market_regime,
                diagnostics_json = excluded.diagnostics_json,
                updated_at = excluded.updated_at
            """,
            (
                self.run_id,
                trade_key,
                signal_key,
                symbol,
                _text(trade.get("timeframe")),
                _text(trade.get("side")),
                _text(trade.get("state")),
                _float_or_none(trade.get("entry_price")),
                _float_or_none(trade.get("exit_price")),
                _float_or_none(trade.get("initial_sl")),
                _float_or_none(trade.get("final_sl")),
                _text(trade.get("exit_reason")),
                _float_or_none(trade.get("r_result")),
                _float_or_none(trade.get("max_gain_r")),
                _float_or_none(trade.get("max_drawdown_r")),
                _int_or_none(trade.get("bars_in_trade")),
                _float_or_none(trade.get("duration_minutes")),
                1 if trade.get("moved_to_breakeven") else 0,
                _text(trade.get("entry_time")),
                _text(trade.get("exit_time")),
                _text(signal_kind),
                _text(btc_regime),
                _text(market_regime),
                _json_text(trade.get("diagnostics_json")),
                now,
                now,
            ),
        )

        self._touch_run()
        self.conn.commit()
