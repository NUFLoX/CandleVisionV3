from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .deferred_entry import (
    DeferredEntryCandidate,
    TRANSIENT_ENTRY_BLOCK_REASONS,
)
from .deferred_entry_service import (
    DeferredEntryCoordinator,
    DeferredEntryRegistration,
)


@dataclass(frozen=True)
class DeferredEntryRuntimeConfig:
    enabled: bool = False
    ttl_hours: float = 24.0
    h1_only: bool = True
    early_statuses: tuple[str, ...] = (
        "PRE_IMPULSE",
        "BREAKOUT_PRESSURE",
        "PENDING",
    )
    early_kinds: tuple[str, ...] = (
        "PRE_IMPULSE_ZONE",
        "BREAKOUT_PRESSURE",
        "ACCUMULATION_LONG_READY",
    )
    min_early_score: float = 10.0
    blocked_btc_regimes: tuple[str, ...] = (
        "BTC_BEARISH",
        "BTC_DUMP_RISK",
    )


@dataclass(frozen=True)
class DeferredEntryProbeResult:
    allowed: bool
    reason: str


@dataclass(frozen=True)
class DeferredEntryRegistrationResult:
    registration: DeferredEntryRegistration | None
    reason: str

    @property
    def registered(self) -> bool:
        return self.registration is not None


class DeferredEntryRuntime:
    """Paper-only admission policy for deferred-entry candidates.

    The runner will eventually supply real executor decisions and snapshots.
    This adapter intentionally does not evaluate markets, run timers, or open
    positions. It only decides whether a blocked setup may be persisted.
    """

    def __init__(
        self,
        coordinator: DeferredEntryCoordinator,
        *,
        config: DeferredEntryRuntimeConfig | None = None,
    ) -> None:
        self.coordinator = coordinator
        self.config = config or DeferredEntryRuntimeConfig()

    @staticmethod
    def _utc_now(now: datetime | None = None) -> datetime:
        value = now or datetime.now(timezone.utc)

        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc)

    @staticmethod
    def _is_h1_timeframe(timeframe: str) -> bool:
        return str(timeframe or "").strip().lower() in {
            "60",
            "1h",
            "h1",
        }

    @staticmethod
    def _normalized_values(
        values: tuple[str, ...],
    ) -> set[str]:
        return {
            str(value or "").strip().upper()
            for value in values
            if str(value or "").strip()
        }

    def probe_early_signal(
        self,
        *,
        mode: str,
        timeframe: str,
        side: str,
        signal_kind: str,
        confirmed_status: str | None,
        score: float,
        btc_regime: str,
    ) -> DeferredEntryProbeResult:
        """Decide whether an early signal may be evaluated for persistence only.

        A positive result never authorizes an order. Runner code must still use
        the regular executor only as a WATCH-classification probe.
        """

        if not self.config.enabled:
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_disabled",
            )

        if str(mode or "").strip().lower() != "paper":
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_mode_not_paper",
            )

        if str(side or "").strip() != "Buy":
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_not_buy_side",
            )

        if (
            self.config.h1_only
            and not self._is_h1_timeframe(timeframe)
        ):
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_timeframe_not_h1",
            )

        status = str(confirmed_status or "").strip().upper()
        allowed_statuses = self._normalized_values(
            self.config.early_statuses
        )

        if status not in allowed_statuses:
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_status_not_allowed",
            )

        kind = str(signal_kind or "").strip().upper()
        allowed_kinds = self._normalized_values(
            self.config.early_kinds
        )

        if kind not in allowed_kinds:
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_signal_kind_not_allowed",
            )

        try:
            numeric_score = float(score)
        except (TypeError, ValueError):
            numeric_score = 0.0

        if numeric_score < max(
            float(self.config.min_early_score),
            0.0,
        ):
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_score_below_threshold",
            )

        blocked_btc_regimes = self._normalized_values(
            self.config.blocked_btc_regimes
        )

        if (
            str(btc_regime or "").strip().upper()
            in blocked_btc_regimes
        ):
            return DeferredEntryProbeResult(
                allowed=False,
                reason="deferred_entry_btc_regime_blocked",
            )

        return DeferredEntryProbeResult(
            allowed=True,
            reason="deferred_entry_probe_allowed",
        )

    def register_blocked_setup(
        self,
        *,
        mode: str,
        signal_key: str,
        symbol: str,
        market: str,
        timeframe: str,
        side: str,
        signal_kind: str,
        score: float,
        origin_entry: float,
        origin_stop_loss: float,
        block_reason: str,
        h4_allowed: bool,
        structural_allowed: bool,
        support: float | None = None,
        ema20: float | None = None,
        vwap: float | None = None,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> DeferredEntryRegistrationResult:
        if not self.config.enabled:
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_disabled",
            )

        if str(mode or "").strip().lower() != "paper":
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_mode_not_paper",
            )

        if str(side or "") != "Buy":
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_not_buy_side",
            )

        if (
            self.config.h1_only
            and not self._is_h1_timeframe(timeframe)
        ):
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_timeframe_not_h1",
            )

        if block_reason not in TRANSIENT_ENTRY_BLOCK_REASONS:
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_block_reason_not_transient",
            )

        if not h4_allowed:
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_h4_gate_not_allowed",
            )

        if not structural_allowed:
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_structural_gate_not_allowed",
            )

        ttl_hours = min(
            max(float(self.config.ttl_hours), 1.0),
            72.0,
        )
        created_at = self._utc_now(now)

        candidate = DeferredEntryCandidate(
            signal_key=str(signal_key),
            symbol=str(symbol),
            market=str(market),
            timeframe=str(timeframe),
            side=str(side),
            signal_kind=str(signal_kind),
            origin_entry=float(origin_entry),
            origin_stop_loss=float(origin_stop_loss),
            score=float(score),
            initial_block_reason=str(block_reason),
            created_at=created_at,
            expires_at=created_at + timedelta(hours=ttl_hours),
            origin_support=support,
            origin_ema20=ema20,
            origin_vwap=vwap,
            metadata=dict(metadata or {}),
        )

        registration = self.coordinator.register(candidate)

        if registration is None:
            return DeferredEntryRegistrationResult(
                registration=None,
                reason="deferred_entry_candidate_not_eligible",
            )

        return DeferredEntryRegistrationResult(
            registration=registration,
            reason=(
                "deferred_entry_created"
                if registration.created
                else "deferred_entry_already_exists"
            ),
        )
