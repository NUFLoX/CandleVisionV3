from __future__ import annotations

from typing import Any

from .deferred_entry_runtime import DeferredEntryRuntime
from .trade_executor import OrderflowSnapshot, TradeSetup


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _signal_metadata(
    signal,
    snapshot: OrderflowSnapshot,
    confirmed_status: str | None,
    admission_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_meta = getattr(signal, "meta", {})

    if not isinstance(raw_meta, dict):
        raw_meta = {}

    raw_reasons = getattr(signal, "reasons", [])

    if isinstance(raw_reasons, (list, tuple, set)):
        reasons = [str(item) for item in raw_reasons]
    elif raw_reasons:
        reasons = [str(raw_reasons)]
    else:
        reasons = []

    payload = {
        "source": str(
            getattr(signal, "source", "orderflow")
            or "orderflow"
        ),
        "confirmed_status": str(confirmed_status or ""),
        "signal_meta": dict(raw_meta),
        "signal_reasons": reasons,
        "take_profit_1": _optional_float(
            getattr(signal, "take_profit_1", None)
        ),
        "take_profit_2": _optional_float(
            getattr(signal, "take_profit_2", None)
        ),
        "initial_snapshot": {
            "price": _optional_float(snapshot.price),
            "spread_bps": _optional_float(
                snapshot.spread_bps
            ),
            "buy_flow": _optional_float(
                snapshot.buy_flow
            ),
            "sell_flow": _optional_float(
                snapshot.sell_flow
            ),
            "volume_impulse": _optional_float(
                snapshot.volume_impulse
            ),
            "ask_wall_strength": _optional_float(
                snapshot.ask_wall_strength
            ),
            "support": _optional_float(snapshot.support),
            "resistance": _optional_float(
                snapshot.resistance
            ),
            "ema20": _optional_float(snapshot.ema20),
            "vwap": _optional_float(snapshot.vwap),
        },
    }

    if admission_diagnostics:
        payload["deferred_entry_initial_admission"] = dict(
            admission_diagnostics
        )

    return payload


def register_deferred_watch(
    *,
    runtime: DeferredEntryRuntime,
    mode: str,
    signal_key: str,
    signal,
    setup: TradeSetup,
    snapshot: OrderflowSnapshot,
    market: str,
    block_reason: str,
    confirmed_status: str | None,
    h4_allowed: bool,
    structural_allowed: bool,
    structural_blockers: list[str],
    admission_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Safely register one executor WATCH as a deferred paper candidate.

    This adapter does not evaluate a market, refresh candidates, or open
    positions. It serializes the existing executor context and delegates only
    admission policy to DeferredEntryRuntime.
    """

    context: dict[str, Any] = {
        "deferred_entry_enabled": bool(
            runtime.config.enabled
        ),
        "deferred_entry_registered": False,
        "deferred_entry_registration_reason": None,
        "deferred_entry_structural_blockers": list(
            structural_blockers
        ),
    }

    if not runtime.config.enabled:
        context["deferred_entry_registration_reason"] = (
            "deferred_entry_disabled"
        )
        return context

    try:
        result = runtime.register_blocked_setup(
            mode=str(mode),
            signal_key=str(signal_key),
            symbol=str(setup.symbol),
            market=str(market),
            timeframe=str(setup.timeframe),
            side=str(setup.side),
            signal_kind=str(setup.signal_kind),
            score=float(setup.score),
            origin_entry=float(setup.entry_hint),
            origin_stop_loss=float(setup.stop_loss),
            block_reason=str(block_reason),
            h4_allowed=bool(h4_allowed),
            structural_allowed=bool(structural_allowed),
            support=_optional_float(snapshot.support),
            ema20=_optional_float(snapshot.ema20),
            vwap=_optional_float(snapshot.vwap),
            metadata=_signal_metadata(
                signal,
                snapshot,
                confirmed_status,
                admission_diagnostics,
            ),
        )
    except Exception as exc:
        context.update(
            {
                "deferred_entry_registration_reason": (
                    "deferred_entry_registration_error"
                ),
                "deferred_entry_registration_error": str(exc),
            }
        )
        return context

    context["deferred_entry_registration_reason"] = (
        result.reason
    )

    if result.registration is None:
        return context

    record = result.registration.record

    context.update(
        {
            "deferred_entry_registered": True,
            "deferred_entry_status": record["status"],
            "deferred_entry_signal_key": record["signal_key"],
            "deferred_entry_expires_at": record["expires_at"],
            "deferred_entry_created": bool(
                result.registration.created
            ),
            "deferred_entry_initial_block_reason": (
                record["initial_block_reason"]
            ),
        }
    )

    return context
