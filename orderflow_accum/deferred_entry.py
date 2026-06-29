from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BUY = "Buy"

DEFERRED_ENTRY_PENDING = "DEFERRED_ENTRY_PENDING"
DEFERRED_ENTRY_PULLBACK_SEEN = "DEFERRED_ENTRY_PULLBACK_SEEN"
DEFERRED_ENTRY_READY = "DEFERRED_ENTRY_READY"
DEFERRED_ENTRY_ENTERED = "DEFERRED_ENTRY_ENTERED"
DEFERRED_ENTRY_INVALIDATED = "DEFERRED_ENTRY_INVALIDATED"
DEFERRED_ENTRY_EXPIRED = "DEFERRED_ENTRY_EXPIRED"
DEFERRED_ENTRY_CANCELLED = "DEFERRED_ENTRY_CANCELLED"

ACTIVE_DEFERRED_STATUSES = {
    DEFERRED_ENTRY_PENDING,
    DEFERRED_ENTRY_PULLBACK_SEEN,
    DEFERRED_ENTRY_READY,
}

TERMINAL_DEFERRED_STATUSES = {
    DEFERRED_ENTRY_ENTERED,
    DEFERRED_ENTRY_INVALIDATED,
    DEFERRED_ENTRY_EXPIRED,
    DEFERRED_ENTRY_CANCELLED,
}

TRANSIENT_ENTRY_BLOCK_REASONS = {
    "entry_blocked_buy_flow",
    "entry_blocked_volume_impulse",
    "entry_blocked_absorption_weak_confirmation",
    "entry_blocked_ask_wall",
}


@dataclass(frozen=True)
class DeferredEntryCandidate:
    signal_key: str
    symbol: str
    market: str
    timeframe: str
    side: str
    signal_kind: str
    origin_entry: float
    origin_stop_loss: float
    score: float
    initial_block_reason: str
    created_at: datetime
    expires_at: datetime
    origin_support: float | None = None
    origin_ema20: float | None = None
    origin_vwap: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeferredEntryState:
    status: str = DEFERRED_ENTRY_PENDING
    lowest_price: float | None = None
    highest_price: float | None = None
    pullback_seen: bool = False


@dataclass(frozen=True)
class DeferredEntrySnapshot:
    price: float
    buy_flow: float
    sell_flow: float
    volume_impulse: float
    ask_wall_strength: float
    support: float | None = None
    ema20: float | None = None
    vwap: float | None = None
    candle_close: float | None = None


@dataclass(frozen=True)
class DeferredEntryConfig:
    min_pullback_r: float = 0.20
    max_pullback_r: float = 0.90
    min_reclaim_flow_ratio: float = 1.05
    min_reclaim_volume_impulse: float = 0.75
    max_reclaim_ask_wall_strength: float = 0.65
    max_reclaim_entry_above_origin_r: float = 0.25


@dataclass(frozen=True)
class DeferredEntryEvaluation:
    status: str
    reason: str
    allowed_to_enter: bool
    lowest_price: float
    highest_price: float
    pullback_seen: bool
    reclaim_level: float | None
    pullback_r: float
    diagnostics: dict[str, Any] = field(default_factory=dict)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _positive(value: float | None) -> float | None:
    if value is None:
        return None
    parsed = float(value)
    return parsed if parsed > 0 else None


def _reclaim_level(
    candidate: DeferredEntryCandidate,
    snapshot: DeferredEntrySnapshot,
) -> float | None:
    references = [
        _positive(candidate.origin_support),
        _positive(candidate.origin_ema20),
        _positive(candidate.origin_vwap),
        _positive(snapshot.support),
        _positive(snapshot.ema20),
        _positive(snapshot.vwap),
    ]
    valid = [value for value in references if value is not None]
    return max(valid) if valid else None


def deferred_entry_is_eligible(candidate: DeferredEntryCandidate) -> bool:
    return (
        candidate.side == BUY
        and candidate.initial_block_reason in TRANSIENT_ENTRY_BLOCK_REASONS
        and candidate.origin_entry > 0
        and candidate.origin_stop_loss > 0
        and candidate.origin_stop_loss < candidate.origin_entry
    )


def evaluate_deferred_entry(
    candidate: DeferredEntryCandidate,
    state: DeferredEntryState,
    snapshot: DeferredEntrySnapshot,
    *,
    now: datetime | None = None,
    config: DeferredEntryConfig | None = None,
) -> DeferredEntryEvaluation:
    now = _as_utc(now or datetime.now(timezone.utc))
    config = config or DeferredEntryConfig()

    price = float(snapshot.price)
    if price <= 0:
        raise ValueError("deferred entry snapshot price must be positive")

    previous_low = _positive(state.lowest_price) or candidate.origin_entry
    previous_high = _positive(state.highest_price) or candidate.origin_entry
    lowest_price = min(previous_low, price)
    highest_price = max(previous_high, price)

    risk = candidate.origin_entry - candidate.origin_stop_loss
    if risk <= 0:
        raise ValueError("deferred entry initial risk must be positive")

    pullback_r = max(
        (candidate.origin_entry - lowest_price) / risk,
        0.0,
    )
    reclaim_level = _reclaim_level(candidate, snapshot)

    diagnostics: dict[str, Any] = {
        "deferred_entry_signal_key": candidate.signal_key,
        "deferred_entry_initial_block_reason": candidate.initial_block_reason,
        "deferred_entry_origin_entry": candidate.origin_entry,
        "deferred_entry_origin_stop_loss": candidate.origin_stop_loss,
        "deferred_entry_current_price": price,
        "deferred_entry_lowest_price": lowest_price,
        "deferred_entry_highest_price": highest_price,
        "deferred_entry_initial_risk": risk,
        "deferred_entry_pullback_r": pullback_r,
        "deferred_entry_reclaim_level": reclaim_level,
        "deferred_entry_min_pullback_r": config.min_pullback_r,
        "deferred_entry_max_pullback_r": config.max_pullback_r,
    }

    if state.status in TERMINAL_DEFERRED_STATUSES:
        return DeferredEntryEvaluation(
            status=state.status,
            reason="deferred_entry_already_terminal",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=state.pullback_seen,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    if now >= _as_utc(candidate.expires_at):
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_EXPIRED,
            reason="deferred_entry_ttl_expired",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=state.pullback_seen,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    if not deferred_entry_is_eligible(candidate):
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_CANCELLED,
            reason="deferred_entry_initial_block_not_eligible",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=state.pullback_seen,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    if price <= candidate.origin_stop_loss:
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_INVALIDATED,
            reason="deferred_entry_structural_stop_invalidated",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=state.pullback_seen,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    if pullback_r > config.max_pullback_r:
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_CANCELLED,
            reason="deferred_entry_pullback_too_deep",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=state.pullback_seen,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    pullback_seen = bool(
        state.pullback_seen
        or pullback_r >= config.min_pullback_r
    )

    if not pullback_seen:
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_PENDING,
            reason="deferred_entry_waiting_for_controlled_pullback",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=False,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    entry_distance_r = (
        (price - candidate.origin_entry) / risk
    )
    diagnostics["deferred_entry_entry_distance_from_origin_r"] = (
        entry_distance_r
    )

    if entry_distance_r > config.max_reclaim_entry_above_origin_r:
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_CANCELLED,
            reason="deferred_entry_reclaim_too_late",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=True,
            reclaim_level=reclaim_level,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    if reclaim_level is None:
        return DeferredEntryEvaluation(
            status=DEFERRED_ENTRY_PULLBACK_SEEN,
            reason="deferred_entry_missing_reclaim_reference",
            allowed_to_enter=False,
            lowest_price=lowest_price,
            highest_price=highest_price,
            pullback_seen=True,
            reclaim_level=None,
            pullback_r=pullback_r,
            diagnostics=diagnostics,
        )

    reclaim_price_ok = price >= reclaim_level
    flow_ok = (
        snapshot.buy_flow > 0
        and snapshot.buy_flow
        >= snapshot.sell_flow
        * config.min_reclaim_flow_ratio
    )
    volume_ok = (
        snapshot.volume_impulse
        >= config.min_reclaim_volume_impulse
    )
    wall_ok = (
        snapshot.ask_wall_strength
        <= config.max_reclaim_ask_wall_strength
    )

    diagnostics.update(
        {
            "deferred_entry_reclaim_price_ok": reclaim_price_ok,
            "deferred_entry_reclaim_flow_ok": flow_ok,
            "deferred_entry_reclaim_volume_ok": volume_ok,
            "deferred_entry_reclaim_wall_ok": wall_ok,
            "deferred_entry_min_reclaim_flow_ratio": (
                config.min_reclaim_flow_ratio
            ),
            "deferred_entry_min_reclaim_volume_impulse": (
                config.min_reclaim_volume_impulse
            ),
            "deferred_entry_max_reclaim_ask_wall_strength": (
                config.max_reclaim_ask_wall_strength
            ),
        }
    )

    allowed = bool(
        reclaim_price_ok
        and flow_ok
        and volume_ok
        and wall_ok
    )

    return DeferredEntryEvaluation(
        status=(
            DEFERRED_ENTRY_READY
            if allowed
            else DEFERRED_ENTRY_PULLBACK_SEEN
        ),
        reason=(
            "deferred_entry_reclaim_confirmed"
            if allowed
            else "deferred_entry_waiting_for_reclaim_confirmation"
        ),
        allowed_to_enter=allowed,
        lowest_price=lowest_price,
        highest_price=highest_price,
        pullback_seen=True,
        reclaim_level=reclaim_level,
        pullback_r=pullback_r,
        diagnostics=diagnostics,
    )


class DeferredEntryStore:
    """SQLite persistence for deferred paper-entry candidates.

    This store is intentionally standalone in the first commit. It is not yet
    called by the scanner or executor loops.
    """

    def __init__(self, db_path: str = "data/signals.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.ensure_schema()

    def close(self) -> None:
        self.conn.close()

    def ensure_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deferred_entries (
                signal_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                market TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                side TEXT NOT NULL,
                signal_kind TEXT NOT NULL,
                origin_entry REAL NOT NULL,
                origin_stop_loss REAL NOT NULL,
                origin_score REAL NOT NULL,
                initial_block_reason TEXT NOT NULL,
                origin_support REAL,
                origin_ema20 REAL,
                origin_vwap REAL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                lowest_price REAL,
                highest_price REAL,
                pullback_seen INTEGER NOT NULL DEFAULT 0,
                last_reason TEXT,
                last_reclaim_level REAL,
                ready_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                last_snapshot_json TEXT NOT NULL DEFAULT '{}',
                diagnostics_json TEXT NOT NULL DEFAULT '{}',
                revalidation_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_deferred_entries_active
            ON deferred_entries(status, expires_at, updated_at)
            """
        )

        columns = {
            str(row[1])
            for row in self.conn.execute(
                "PRAGMA table_info(deferred_entries)"
            ).fetchall()
        }

        if "revalidation_json" not in columns:
            self.conn.execute(
                """
                ALTER TABLE deferred_entries
                ADD COLUMN revalidation_json
                TEXT NOT NULL DEFAULT '{}'
                """
            )

        self.conn.commit()

    @staticmethod
    def _utc_text(value: datetime) -> str:
        return _as_utc(value).isoformat()

    @staticmethod
    def _safe_json(value: Any) -> str:
        return json.dumps(
            value,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )

    @staticmethod
    def _snapshot_dict(
        snapshot: DeferredEntrySnapshot,
    ) -> dict[str, Any]:
        return {
            "price": snapshot.price,
            "buy_flow": snapshot.buy_flow,
            "sell_flow": snapshot.sell_flow,
            "volume_impulse": snapshot.volume_impulse,
            "ask_wall_strength": snapshot.ask_wall_strength,
            "support": snapshot.support,
            "ema20": snapshot.ema20,
            "vwap": snapshot.vwap,
            "candle_close": snapshot.candle_close,
        }

    @staticmethod
    def _decode_json(value: Any) -> dict[str, Any]:
        if value in (None, ""):
            return {}
        try:
            parsed = json.loads(str(value))
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _row_to_dict(
        self,
        row: sqlite3.Row | None,
    ) -> dict[str, Any] | None:
        if row is None:
            return None

        payload = dict(row)
        for key in (
            "metadata_json",
            "last_snapshot_json",
            "diagnostics_json",
            "revalidation_json",
        ):
            payload[key] = self._decode_json(payload.get(key))
        return payload

    def create_or_get(
        self,
        candidate: DeferredEntryCandidate,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()

        self.conn.execute(
            """
            INSERT INTO deferred_entries (
                signal_key, symbol, market, timeframe, side, signal_kind,
                origin_entry, origin_stop_loss, origin_score,
                initial_block_reason, origin_support, origin_ema20,
                origin_vwap, status, created_at, expires_at, updated_at,
                lowest_price, highest_price, pullback_seen, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(signal_key) DO NOTHING
            """,
            (
                candidate.signal_key,
                candidate.symbol,
                candidate.market,
                candidate.timeframe,
                candidate.side,
                candidate.signal_kind,
                candidate.origin_entry,
                candidate.origin_stop_loss,
                candidate.score,
                candidate.initial_block_reason,
                candidate.origin_support,
                candidate.origin_ema20,
                candidate.origin_vwap,
                DEFERRED_ENTRY_PENDING,
                self._utc_text(candidate.created_at),
                self._utc_text(candidate.expires_at),
                now,
                candidate.origin_entry,
                candidate.origin_entry,
                0,
                self._safe_json(candidate.metadata),
            ),
        )
        self.conn.commit()

        row = self.get(candidate.signal_key)
        if row is None:
            raise RuntimeError(
                "deferred entry candidate was not persisted"
            )
        return row

    def get(
        self,
        signal_key: str,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT *
            FROM deferred_entries
            WHERE signal_key = ?
            """,
            (signal_key,),
        ).fetchone()
        return self._row_to_dict(row)

    def list_active(
        self,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        statuses = sorted(ACTIVE_DEFERRED_STATUSES)
        placeholders = ",".join("?" for _ in statuses)

        rows = self.conn.execute(
            f"""
            SELECT *
            FROM deferred_entries
            WHERE status IN ({placeholders})
            ORDER BY updated_at ASC
            LIMIT ?
            """,
            (*statuses, max(int(limit), 1)),
        ).fetchall()

        return [
            self._row_to_dict(row)
            for row in rows
            if row is not None
        ]

    def apply_evaluation(
        self,
        signal_key: str,
        evaluation: DeferredEntryEvaluation,
        snapshot: DeferredEntrySnapshot,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()

        cursor = self.conn.execute(
            """
            UPDATE deferred_entries
            SET
                status = ?,
                updated_at = ?,
                lowest_price = ?,
                highest_price = ?,
                pullback_seen = ?,
                last_reason = ?,
                last_reclaim_level = ?,
                ready_at = CASE
                    WHEN ? = 1 THEN COALESCE(ready_at, ?)
                    ELSE ready_at
                END,
                last_snapshot_json = ?,
                diagnostics_json = ?
            WHERE signal_key = ?
            """,
            (
                evaluation.status,
                now,
                evaluation.lowest_price,
                evaluation.highest_price,
                int(evaluation.pullback_seen),
                evaluation.reason,
                evaluation.reclaim_level,
                int(
                    evaluation.status
                    == DEFERRED_ENTRY_READY
                ),
                now,
                self._safe_json(
                    self._snapshot_dict(snapshot)
                ),
                self._safe_json(evaluation.diagnostics),
                signal_key,
            ),
        )
        self.conn.commit()

        if cursor.rowcount != 1:
            raise KeyError(
                f"unknown deferred entry: {signal_key}"
            )

        row = self.get(signal_key)
        if row is None:
            raise RuntimeError(
                "deferred entry disappeared after update"
            )
        return row

    def record_revalidation(
        self,
        signal_key: str,
        *,
        allowed_to_enter: bool,
        reason: str,
        diagnostics: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Persist the latest strict revalidation without changing lifecycle."""

        record = self.get(signal_key)

        if record is None:
            raise KeyError(
                f"unknown deferred entry: {signal_key}"
            )

        if str(record.get("status") or "") != DEFERRED_ENTRY_READY:
            return None

        previous = record.get("revalidation_json") or {}

        if not isinstance(previous, dict):
            previous = {}

        try:
            attempts = max(
                int(previous.get("attempt_count") or 0),
                0,
            )
        except (TypeError, ValueError):
            attempts = 0

        now = datetime.now(timezone.utc).isoformat()

        payload = {
            "attempt_count": attempts + 1,
            "allowed_to_enter": bool(allowed_to_enter),
            "reason": str(reason or ""),
            "recorded_at": now,
            "diagnostics": dict(diagnostics or {}),
        }

        cursor = self.conn.execute(
            """
            UPDATE deferred_entries
            SET
                updated_at = ?,
                last_reason = ?,
                revalidation_json = ?
            WHERE signal_key = ?
              AND status = ?
            """,
            (
                now,
                str(reason or ""),
                self._safe_json(payload),
                signal_key,
                DEFERRED_ENTRY_READY,
            ),
        )
        self.conn.commit()

        if cursor.rowcount != 1:
            return None

        return self.get(signal_key)
