from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .deferred_entry import DEFERRED_ENTRY_READY
from .deferred_entry_revalidation import (
    DeferredEntryRevalidationResult,
)
from .deferred_entry_service import DeferredEntryCoordinator


@dataclass(frozen=True)
class DeferredEntryRevalidationBatch:
    attempted: int
    persisted: int
    skipped_missing: tuple[str, ...]
    skipped_not_ready: tuple[str, ...]
    records: tuple[dict[str, object], ...]


class DeferredEntryRevalidationService:
    """Persist strict READY revalidation results without opening positions."""

    def __init__(
        self,
        coordinator: DeferredEntryCoordinator,
    ) -> None:
        self.coordinator = coordinator

    def persist_ready_results(
        self,
        results: Iterable[DeferredEntryRevalidationResult],
    ) -> DeferredEntryRevalidationBatch:
        attempted = 0
        persisted = 0
        missing: list[str] = []
        not_ready: list[str] = []
        records: list[dict[str, object]] = []

        for result in results:
            attempted += 1
            signal_key = str(result.signal_key or "")
            record = self.coordinator.store.get(signal_key)

            if record is None:
                missing.append(signal_key)
                continue

            if (
                str(record.get("status") or "")
                != DEFERRED_ENTRY_READY
            ):
                not_ready.append(signal_key)
                continue

            updated = self.coordinator.store.record_revalidation(
                signal_key,
                allowed_to_enter=result.allowed_to_enter,
                reason=result.reason,
                diagnostics=result.diagnostics,
            )

            if updated is None:
                not_ready.append(signal_key)
                continue

            persisted += 1
            records.append(updated)

        return DeferredEntryRevalidationBatch(
            attempted=attempted,
            persisted=persisted,
            skipped_missing=tuple(missing),
            skipped_not_ready=tuple(not_ready),
            records=tuple(records),
        )
