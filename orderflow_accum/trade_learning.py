from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


SUPPORTED_EVENT_TYPES = {
    "SIGNAL_CREATED",
    "SIGNAL_UPDATED",
    "CONFIRMED",
    "EXECUTOR_WATCH",
    "EXECUTOR_ENTER",
    "EXECUTOR_BREAKEVEN",
    "EXECUTOR_HOLD",
    "EXECUTOR_EXIT",
    "OUTCOME_TP",
    "OUTCOME_SL",
    "OUTCOME_EXPIRED",
    "OUTCOME_AMBIGUOUS",
}


@dataclass(slots=True)
class TradeLifecycleEvent:
    signal_key: str
    symbol: str
    timeframe: str
    side: str
    event_type: str
    status: str | None = None
    action: str | None = None
    reason: str | None = None
    price: float | None = None
    score: float | None = None
    btc_regime: str | None = None
    market_regime: str | None = None
    features: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None


class TradeLearningEngine:
    """Best-effort lifecycle memory recorder.

    This engine only converts signal/executor context into durable lifecycle
    events. It does not make decisions, alter thresholds, place orders, or
    mutate signal/executor state.
    """

    def __init__(self, store: Any, logger: logging.Logger | None = None) -> None:
        self.store = store
        self.logger = logger or logging.getLogger("Accum.TradeLearning")

    def record_event(self, event: TradeLifecycleEvent | dict[str, Any]) -> None:
        try:
            self.store.add_trade_lifecycle_event(event)
        except Exception as exc:
            self.logger.warning("Trade learning persistence failed: %r", exc, exc_info=True)

    def record_signal(
        self,
        *,
        signal: Any,
        signal_key: str,
        event_type: str,
        status: str | None,
        features: dict[str, Any] | None = None,
    ) -> None:
        meta = dict(getattr(signal, "meta", {}) or {})
        self.record_event(
            TradeLifecycleEvent(
                signal_key=signal_key,
                symbol=str(getattr(signal, "symbol", "UNKNOWN")),
                timeframe=str(meta.get("tf") or ""),
                side=str(getattr(signal, "side", "")),
                event_type=event_type,
                status=status,
                price=_safe_float(getattr(signal, "entry", None)),
                score=_safe_float(getattr(signal, "score", None)),
                btc_regime=_optional_str(meta.get("btc_regime")),
                market_regime=_optional_str(meta.get("market_regime")),
                features=features or self._signal_features(signal, meta),
                created_at=_utc_now(),
            )
        )

    def record_executor_decision(
        self,
        *,
        signal: Any,
        signal_key: str,
        state: str | None,
        action: str | None,
        reason: str | None,
        price: float | None = None,
        features: dict[str, Any] | None = None,
    ) -> None:
        event_type = self.executor_event_type(action)
        if event_type is None:
            return

        meta = dict(getattr(signal, "meta", {}) or {})
        self.record_event(
            TradeLifecycleEvent(
                signal_key=signal_key,
                symbol=str(getattr(signal, "symbol", "UNKNOWN")),
                timeframe=str(meta.get("tf") or ""),
                side=str(getattr(signal, "side", "")),
                event_type=event_type,
                status=state,
                action=action,
                reason=reason,
                price=price if price is not None else _safe_float(getattr(signal, "entry", None)),
                score=_safe_float(getattr(signal, "score", None)),
                btc_regime=_optional_str(meta.get("btc_regime")),
                market_regime=_optional_str(meta.get("market_regime")),
                features=features or {},
                created_at=_utc_now(),
            )
        )

    @staticmethod
    def executor_event_type(action: str | None) -> str | None:
        action_u = str(action or "").upper()
        if action_u == "WATCH":
            return "EXECUTOR_WATCH"
        if action_u in {"ENTER_LONG", "ENTER_SHORT"}:
            return "EXECUTOR_ENTER"
        if action_u == "MOVE_SL_TO_BREAKEVEN":
            return "EXECUTOR_BREAKEVEN"
        if action_u == "HOLD":
            return "EXECUTOR_HOLD"
        if action_u == "EXIT":
            return "EXECUTOR_EXIT"
        return None

    @staticmethod
    def _signal_features(signal: Any, meta: dict[str, Any]) -> dict[str, Any]:
        return {
            "kind": getattr(signal, "kind", None),
            "source": getattr(signal, "source", None),
            "market": meta.get("market"),
            "reasons": list(getattr(signal, "reasons", []) or []),
            "entry": _safe_float(getattr(signal, "entry", None)),
            "stop_loss": _safe_float(getattr(signal, "stop_loss", None)),
            "take_profit_1": _safe_float(getattr(signal, "take_profit_1", None)),
            "take_profit_2": _safe_float(getattr(signal, "take_profit_2", None)),
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
