from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Protocol

from .deferred_entry import DEFERRED_ENTRY_READY
from .trade_executor import (
    ENTER_LONG,
    OrderflowSnapshot,
    TradeDecision,
    TradeSetup,
)


class StrictEntryEvaluator(Protocol):
    def evaluate_entry(
        self,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
    ) -> TradeDecision:
        ...


@dataclass(frozen=True)
class DeferredEntryRevalidationResult:
    signal_key: str
    allowed_to_enter: bool
    reason: str
    executor_decision: TradeDecision | None
    diagnostics: dict[str, Any] = field(default_factory=dict)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _is_h1(value: Any) -> bool:
    return _text(value).lower() in {"60", "1h", "h1"}


def _snapshot_diagnostics(
    snapshot: OrderflowSnapshot,
) -> dict[str, Any]:
    return {
        "deferred_entry_revalidation_price": float(snapshot.price),
        "deferred_entry_revalidation_spread_bps": float(
            snapshot.spread_bps
        ),
        "deferred_entry_revalidation_buy_flow": float(
            snapshot.buy_flow
        ),
        "deferred_entry_revalidation_sell_flow": float(
            snapshot.sell_flow
        ),
        "deferred_entry_revalidation_volume_impulse": float(
            snapshot.volume_impulse
        ),
        "deferred_entry_revalidation_ask_wall_strength": float(
            snapshot.ask_wall_strength
        ),
        "deferred_entry_revalidation_support": snapshot.support,
        "deferred_entry_revalidation_ema20": snapshot.ema20,
        "deferred_entry_revalidation_vwap": snapshot.vwap,
    }


def revalidate_ready_deferred_entry(
    *,
    record: Mapping[str, Any],
    mode: str,
    setup: TradeSetup,
    snapshot: OrderflowSnapshot,
    snapshot_fresh: bool,
    h4_allowed: bool,
    h4_reason: str | None,
    executor: StrictEntryEvaluator,
    guard_decisions: Iterable[
        tuple[str, TradeDecision | None]
    ] = (),
) -> DeferredEntryRevalidationResult:
    """Re-run strict executor admission for a READY deferred candidate.

    This is a pure decision layer. It never persists state, fetches market
    data, mutates a signal, opens a position, or relaxes executor gates.
    """

    signal_key = _text(record.get("signal_key"))
    record_status = _text(record.get("status"))
    record_side = _text(record.get("side"))
    record_timeframe = _text(record.get("timeframe"))
    normalized_mode = _text(mode).lower()

    diagnostics: dict[str, Any] = {
        "deferred_entry_revalidation_status": record_status,
        "deferred_entry_revalidation_mode": normalized_mode,
        "deferred_entry_revalidation_snapshot_fresh": bool(
            snapshot_fresh
        ),
        "deferred_entry_revalidation_h4_allowed": bool(h4_allowed),
        "deferred_entry_revalidation_h4_reason": _text(h4_reason)
        or None,
        **_snapshot_diagnostics(snapshot),
    }

    if record_status != DEFERRED_ENTRY_READY:
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason="deferred_entry_revalidation_not_ready",
            executor_decision=None,
            diagnostics=diagnostics,
        )

    if normalized_mode != "paper":
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason="deferred_entry_revalidation_mode_not_paper",
            executor_decision=None,
            diagnostics=diagnostics,
        )

    if (
        record_side != "Buy"
        or _text(setup.side) != "Buy"
    ):
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason="deferred_entry_revalidation_not_buy_side",
            executor_decision=None,
            diagnostics=diagnostics,
        )

    if (
        not _is_h1(record_timeframe)
        or not _is_h1(setup.timeframe)
    ):
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason="deferred_entry_revalidation_timeframe_not_h1",
            executor_decision=None,
            diagnostics=diagnostics,
        )

    if not snapshot_fresh:
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason=(
                "deferred_entry_revalidation_missing_fresh_orderflow"
            ),
            executor_decision=None,
            diagnostics=diagnostics,
        )

    if float(snapshot.price) <= 0:
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason="deferred_entry_revalidation_invalid_snapshot_price",
            executor_decision=None,
            diagnostics=diagnostics,
        )

    if not h4_allowed:
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason=_text(h4_reason)
            or "entry_blocked_h4_bearish_structure",
            executor_decision=None,
            diagnostics=diagnostics,
        )

    entry_decision = executor.evaluate_entry(setup, snapshot)
    diagnostics.update(
        {
            "deferred_entry_revalidation_executor_action": (
                _text(entry_decision.action)
            ),
            "deferred_entry_revalidation_executor_reason": (
                _text(entry_decision.reason)
            ),
            "deferred_entry_revalidation_executor_state": (
                _text(entry_decision.next_state)
            ),
        }
    )

    if _text(entry_decision.action) != ENTER_LONG:
        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason=_text(entry_decision.reason)
            or "deferred_entry_revalidation_executor_watch",
            executor_decision=entry_decision,
            diagnostics=diagnostics,
        )

    for guard_name, guard_decision in guard_decisions:
        if guard_decision is None:
            continue

        normalized_guard_name = _text(guard_name) or "unknown_guard"
        guard_reason = _text(guard_decision.reason)
        diagnostics.update(
            {
                "deferred_entry_revalidation_blocking_guard": (
                    normalized_guard_name
                ),
                "deferred_entry_revalidation_guard_reason": (
                    guard_reason or None
                ),
            }
        )

        return DeferredEntryRevalidationResult(
            signal_key=signal_key,
            allowed_to_enter=False,
            reason=guard_reason
            or (
                "deferred_entry_revalidation_"
                f"{normalized_guard_name}_blocked"
            ),
            executor_decision=entry_decision,
            diagnostics=diagnostics,
        )

    diagnostics["deferred_entry_revalidation_allowed"] = True

    return DeferredEntryRevalidationResult(
        signal_key=signal_key,
        allowed_to_enter=True,
        reason="deferred_entry_revalidation_strict_gates_passed",
        executor_decision=entry_decision,
        diagnostics=diagnostics,
    )
