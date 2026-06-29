from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .deferred_entry import (
    DeferredEntryCandidate,
    DeferredEntryConfig,
    DeferredEntryEvaluation,
    DeferredEntrySnapshot,
    DeferredEntryState,
    DeferredEntryStore,
    deferred_entry_is_eligible,
    evaluate_deferred_entry,
)


@dataclass(frozen=True)
class DeferredEntryRegistration:
    candidate: DeferredEntryCandidate
    record: dict[str, Any]
    created: bool


@dataclass(frozen=True)
class DeferredEntryRefresh:
    candidate: DeferredEntryCandidate
    evaluation: DeferredEntryEvaluation
    record: dict[str, Any]
    state_changed: bool


class DeferredEntryCoordinator:
    """Persistence-aware coordinator for paper deferred-entry candidates.

    This module deliberately contains no scanner, WebSocket, REST, executor,
    or order-routing logic. The runner integration will supply snapshots later.
    """

    def __init__(
        self,
        store: DeferredEntryStore,
        *,
        config: DeferredEntryConfig | None = None,
    ) -> None:
        self.store = store
        self.config = config or DeferredEntryConfig()

    @staticmethod
    def _parse_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value or "").strip()
            if not text:
                raise ValueError(
                    "deferred entry record is missing datetime"
                )
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            parsed = datetime.fromisoformat(text)

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _positive_float(value: Any) -> float | None:
        if value is None:
            return None

        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None

        return parsed if parsed > 0 else None

    @classmethod
    def candidate_from_record(
        cls,
        record: dict[str, Any],
    ) -> DeferredEntryCandidate:
        metadata = record.get("metadata_json") or {}

        if not isinstance(metadata, dict):
            metadata = {}

        return DeferredEntryCandidate(
            signal_key=str(record["signal_key"]),
            symbol=str(record["symbol"]),
            market=str(record["market"]),
            timeframe=str(record["timeframe"]),
            side=str(record["side"]),
            signal_kind=str(record["signal_kind"]),
            origin_entry=float(record["origin_entry"]),
            origin_stop_loss=float(record["origin_stop_loss"]),
            score=float(record["origin_score"]),
            initial_block_reason=str(
                record["initial_block_reason"]
            ),
            created_at=cls._parse_datetime(
                record["created_at"]
            ),
            expires_at=cls._parse_datetime(
                record["expires_at"]
            ),
            origin_support=cls._positive_float(
                record.get("origin_support")
            ),
            origin_ema20=cls._positive_float(
                record.get("origin_ema20")
            ),
            origin_vwap=cls._positive_float(
                record.get("origin_vwap")
            ),
            metadata=dict(metadata),
        )

    @classmethod
    def state_from_record(
        cls,
        record: dict[str, Any],
    ) -> DeferredEntryState:
        return DeferredEntryState(
            status=str(
                record.get("status")
                or "DEFERRED_ENTRY_PENDING"
            ),
            lowest_price=cls._positive_float(
                record.get("lowest_price")
            ),
            highest_price=cls._positive_float(
                record.get("highest_price")
            ),
            pullback_seen=bool(
                record.get("pullback_seen")
            ),
        )

    def register(
        self,
        candidate: DeferredEntryCandidate,
    ) -> DeferredEntryRegistration | None:
        """Persist only eligible transient paper-entry candidates."""

        if not deferred_entry_is_eligible(candidate):
            return None

        existing = self.store.get(candidate.signal_key)
        record = self.store.create_or_get(candidate)

        return DeferredEntryRegistration(
            candidate=candidate,
            record=record,
            created=existing is None,
        )

    def refresh(
        self,
        record: dict[str, Any],
        snapshot: DeferredEntrySnapshot,
        *,
        now: datetime | None = None,
    ) -> DeferredEntryRefresh:
        """Evaluate and persist one candidate using a supplied fresh snapshot."""

        candidate = self.candidate_from_record(record)
        state = self.state_from_record(record)

        evaluation = evaluate_deferred_entry(
            candidate,
            state,
            snapshot,
            now=now,
            config=self.config,
        )

        updated = self.store.apply_evaluation(
            candidate.signal_key,
            evaluation,
            snapshot,
        )

        return DeferredEntryRefresh(
            candidate=candidate,
            evaluation=evaluation,
            record=updated,
            state_changed=(
                str(record.get("status") or "")
                != str(updated.get("status") or "")
            ),
        )
