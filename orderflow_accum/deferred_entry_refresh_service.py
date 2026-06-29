from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from .deferred_entry import (
    DEFERRED_ENTRY_EXPIRED,
    DEFERRED_ENTRY_INVALIDATED,
    DeferredEntrySnapshot,
)
from .deferred_entry_service import (
    DeferredEntryCoordinator,
    DeferredEntryRefresh,
)


@dataclass(frozen=True)
class DeferredEntryRefreshBatch:
    """Result of updating deferred candidates from caller-provided snapshots.

    This service deliberately has no REST/WebSocket/executor/order dependencies.
    A future runner adapter will supply fresh H1 structure and live orderflow
    snapshots, then decide how a READY candidate is handled.
    """

    attempted: int
    refreshed: int
    skipped_missing_snapshot_keys: tuple[str, ...]
    ready_signal_keys: tuple[str, ...]
    terminal_signal_keys: tuple[str, ...]
    updates: tuple[DeferredEntryRefresh, ...]


class DeferredEntryRefreshService:
    """Bounded persistence refresh for active deferred-entry candidates."""

    def __init__(
        self,
        coordinator: DeferredEntryCoordinator,
        *,
        max_active: int = 12,
    ) -> None:
        self.coordinator = coordinator
        self.max_active = min(
            max(int(max_active), 1),
            200,
        )

    def refresh_active(
        self,
        snapshots_by_signal_key: Mapping[
            str,
            DeferredEntrySnapshot,
        ],
        *,
        now: datetime | None = None,
    ) -> DeferredEntryRefreshBatch:
        """Persist state transitions for active candidates with fresh snapshots.

        Missing snapshots are skipped without changing the candidate. READY
        remains only a persisted reclaim state; this method cannot open trades.
        """

        records = self.coordinator.store.list_active(
            limit=self.max_active,
        )

        skipped: list[str] = []
        ready: list[str] = []
        terminal: list[str] = []
        updates: list[DeferredEntryRefresh] = []

        for record in records:
            signal_key = str(record["signal_key"])
            snapshot = snapshots_by_signal_key.get(signal_key)

            if snapshot is None:
                skipped.append(signal_key)
                continue

            update = self.coordinator.refresh(
                record,
                snapshot,
                now=now,
            )
            updates.append(update)

            if update.evaluation.allowed_to_enter:
                ready.append(signal_key)

            if update.evaluation.status in {
                DEFERRED_ENTRY_INVALIDATED,
                DEFERRED_ENTRY_EXPIRED,
            }:
                terminal.append(signal_key)

        return DeferredEntryRefreshBatch(
            attempted=len(records),
            refreshed=len(updates),
            skipped_missing_snapshot_keys=tuple(skipped),
            ready_signal_keys=tuple(ready),
            terminal_signal_keys=tuple(terminal),
            updates=tuple(updates),
        )
