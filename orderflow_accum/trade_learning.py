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

OUTCOME_EVENT_TYPES = {
    "TP1": "OUTCOME_TP",
    "TP2": "OUTCOME_TP",
    "SL": "OUTCOME_SL",
    "EXPIRED": "OUTCOME_EXPIRED",
    "AMBIGUOUS": "OUTCOME_AMBIGUOUS",
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

    This engine only converts signal/executor/outcome context into durable
    memory. It does not make decisions, alter thresholds, place orders, or
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

    def record_outcome(
        self,
        signal: Any,
        signal_key: str,
        outcome: str,
        outcome_features: dict[str, Any],
    ) -> None:
        """Record final outcome lifecycle memory and a basic diagnosis.

        PENDING is intentionally ignored. All writes are best-effort so callers
        such as outcome_tracker never fail because learning memory failed.
        """
        try:
            outcome_u = str(outcome or "").upper()
            event_type = OUTCOME_EVENT_TYPES.get(outcome_u)
            if event_type is None:
                return

            meta = dict(getattr(signal, "meta", {}) or {})
            features = dict(outcome_features or {})
            timeframe = str(features.get("timeframe") or meta.get("tf") or "")
            side = str(features.get("side") or getattr(signal, "side", ""))
            symbol = str(features.get("symbol") or getattr(signal, "symbol", "UNKNOWN"))
            score = _safe_float(features.get("score", getattr(signal, "score", None)))

            if not self.store.has_trade_lifecycle_event(signal_key, event_type):
                self.store.add_trade_lifecycle_event(
                    TradeLifecycleEvent(
                        signal_key=signal_key,
                        symbol=symbol,
                        timeframe=timeframe,
                        side=side,
                        event_type=event_type,
                        status=outcome_u,
                        price=_safe_float(features.get("entry", getattr(signal, "entry", None))),
                        score=score,
                        btc_regime=_optional_str(meta.get("btc_regime")),
                        market_regime=_optional_str(meta.get("market_regime")),
                        features=features,
                        created_at=_utc_now(),
                    )
                )

            self.store.upsert_trade_diagnosis(
                self._build_diagnosis(
                    signal=signal,
                    signal_key=signal_key,
                    outcome=outcome_u,
                    event_type=event_type,
                    features=features,
                    symbol=symbol,
                    timeframe=timeframe,
                    side=side,
                    score=score,
                )
            )
        except Exception as exc:
            self.logger.warning("Trade outcome learning failed: %r", exc, exc_info=True)

    def _build_diagnosis(
        self,
        *,
        signal: Any,
        signal_key: str,
        outcome: str,
        event_type: str,
        features: dict[str, Any],
        symbol: str,
        timeframe: str,
        side: str,
        score: float | None,
    ) -> dict[str, Any]:
        raw_reasons = features.get("reasons")
        if not isinstance(raw_reasons, list):
            raw_reasons = getattr(signal, "reasons", []) or []
        reasons = list(raw_reasons)
        success_factors: dict[str, Any] = {}
        failure_factors: dict[str, Any] = {}
        recommendation = None

        if event_type == "OUTCOME_TP":
            diagnosis = "Signal reached take profit. Setup direction was correct."
            success_factors = _compact_dict(
                {
                    "score": score,
                    "reasons": reasons,
                    "timeframe": timeframe,
                    "max_gain_pct": _safe_float(features.get("max_gain_pct")),
                    "time_to_tp1_minutes": _safe_float(features.get("time_to_tp1_minutes")),
                    "time_to_tp2_minutes": _safe_float(features.get("time_to_tp2_minutes")),
                }
            )
        elif event_type == "OUTCOME_SL":
            diagnosis = "Signal hit stop loss. Setup failed before reaching take profit."
            failure_factors = _compact_dict(
                {
                    "max_drawdown_pct": _safe_float(features.get("max_drawdown_pct")),
                    "time_to_sl_minutes": _safe_float(features.get("time_to_sl_minutes")),
                    "reasons": reasons,
                    "timeframe": timeframe,
                }
            )
            recommendation = "Review entry timing, stop placement, and weak reasons for this symbol/timeframe."
        elif event_type == "OUTCOME_EXPIRED":
            diagnosis = "Signal expired without decisive TP/SL outcome."
            recommendation = "Review whether timeout window or confirmation threshold is too strict."
        else:
            diagnosis = "Signal produced ambiguous outcome. Price action touched conflicting levels or insufficient order was available."
            recommendation = "Review OHLC replay ordering and volatility conditions."

        return {
            "signal_key": signal_key,
            "symbol": symbol,
            "timeframe": timeframe,
            "side": side,
            "outcome": outcome,
            "diagnosis": diagnosis,
            "success_factors": success_factors,
            "failure_factors": failure_factors,
            "recommendation": recommendation,
            "r_result": _rough_r_result(signal, outcome, features),
            "max_gain_pct": _safe_float(features.get("max_gain_pct")),
            "max_drawdown_pct": _safe_float(features.get("max_drawdown_pct")),
            "time_to_tp1_minutes": _safe_float(features.get("time_to_tp1_minutes")),
            "time_to_tp2_minutes": _safe_float(features.get("time_to_tp2_minutes")),
            "time_to_sl_minutes": _safe_float(features.get("time_to_sl_minutes")),
            "updated_at": _utc_now(),
        }

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


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in value.items() if v is not None}


def _feature_or_attr(signal: Any, features: dict[str, Any], feature_name: str, attr_name: str) -> float | None:
    return _safe_float(features.get(feature_name, getattr(signal, attr_name, None)))


def _rough_r_result(signal: Any, outcome: str, features: dict[str, Any]) -> float | None:
    outcome_u = str(outcome or "").upper()
    if outcome_u == "SL":
        return -1.0
    if outcome_u in {"EXPIRED", "AMBIGUOUS"}:
        return 0.0
    if outcome_u not in {"TP1", "TP2"}:
        return None

    entry = _feature_or_attr(signal, features, "entry", "entry")
    sl = _feature_or_attr(signal, features, "stop_loss", "stop_loss")
    tp_attr = "take_profit_2" if outcome_u == "TP2" else "take_profit_1"
    tp = _feature_or_attr(signal, features, tp_attr, tp_attr)
    if entry is None or sl is None or tp is None:
        return None

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    side = str(features.get("side") or getattr(signal, "side", "Buy")).lower()
    reward = entry - tp if side == "sell" else tp - entry
    if reward <= 0:
        return None
    return reward / risk
