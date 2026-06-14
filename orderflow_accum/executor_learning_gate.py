from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable


ENTRY_BLOCKED_LEARNING_SYMBOL_UNDERPERFORMANCE = "entry_blocked_learning_symbol_underperformance"
ENTRY_BLOCKED_LEARNING_SETUP_UNDERPERFORMANCE = "entry_blocked_learning_setup_underperformance"
ENTRY_BLOCKED_LEARNING_RECENT_CHURN = "entry_blocked_learning_recent_churn"
ENTRY_BLOCKED_LEARNING_LOSS_STREAK = "entry_blocked_learning_loss_streak"

CHURN_EXIT_REASONS = {
    "exit_sell_flow_dominance",
    "exit_below_ema20_with_selling",
    "exit_stop_loss_hit",
}


@dataclass(frozen=True)
class ExecutorLearningGateConfig:
    enabled: bool = True
    lookback_hours: float = 72.0
    min_symbol_trades: int = 8
    min_setup_trades: int = 5
    min_profit_factor: float = 1.05
    min_avg_adjusted_r: float = 0.005
    cooldown_minutes: float = 360.0
    recent_loss_streak: int = 4
    recent_churn_lookback_minutes: float = 120.0
    recent_churn_min_exits: int = 3
    taker_fee_one_side: float = 0.00055
    slippage_one_side: float = 0.00020

    @classmethod
    def from_env(cls) -> "ExecutorLearningGateConfig":
        return cls(
            enabled=_env_bool("EXECUTOR_LEARNING_GATE", True),
            lookback_hours=_env_float("EXECUTOR_LEARNING_LOOKBACK_HOURS", 72.0),
            min_symbol_trades=_env_int("EXECUTOR_LEARNING_MIN_SYMBOL_TRADES", 8),
            min_setup_trades=_env_int("EXECUTOR_LEARNING_MIN_SETUP_TRADES", 5),
            min_profit_factor=_env_float("EXECUTOR_LEARNING_MIN_PROFIT_FACTOR", 1.05),
            min_avg_adjusted_r=_env_float("EXECUTOR_LEARNING_MIN_AVG_ADJUSTED_R", 0.005),
            cooldown_minutes=_env_float("EXECUTOR_LEARNING_COOLDOWN_MINUTES", 360.0),
            recent_loss_streak=_env_int("EXECUTOR_LEARNING_RECENT_LOSS_STREAK", 4),
            recent_churn_lookback_minutes=_env_float("EXECUTOR_LEARNING_RECENT_CHURN_LOOKBACK_MINUTES", 120.0),
            recent_churn_min_exits=_env_int("EXECUTOR_LEARNING_RECENT_CHURN_MIN_EXITS", 3),
            taker_fee_one_side=_env_float("EXECUTOR_TAKER_FEE_ONE_SIDE", 0.00055),
            slippage_one_side=_env_float("EXECUTOR_SLIPPAGE_ONE_SIDE", 0.00020),
        )

    @property
    def roundtrip_cost_pct(self) -> float:
        return 2.0 * (max(float(self.taker_fee_one_side), 0.0) + max(float(self.slippage_one_side), 0.0))


@dataclass(frozen=True)
class LearningStats:
    trade_count: int = 0
    adjusted_sum_r: float = 0.0
    avg_adjusted_r: float = 0.0
    adjusted_profit_factor: float = 0.0
    adjusted_winrate: float = 0.0


@dataclass(frozen=True)
class LearningTrade:
    symbol: str
    signal_kind: str
    adjusted_r: float
    exit_reason: str
    exit_time: datetime | None


@dataclass(frozen=True)
class ExecutorLearningGateDecision:
    blocked: bool
    reason: str | None
    diagnostics: dict[str, object]


class ExecutorLearningGate:
    def __init__(self, config: ExecutorLearningGateConfig | None = None) -> None:
        self.config = config or ExecutorLearningGateConfig.from_env()

    def evaluate(self, setup, historical_trades: Iterable[dict[str, Any]], *, now: datetime | None = None) -> ExecutorLearningGateDecision:
        if not self.config.enabled:
            return ExecutorLearningGateDecision(False, None, {})

        current_time = (now or datetime.now(UTC)).astimezone(UTC)
        symbol = str(getattr(setup, "symbol", "") or "").strip().upper()
        signal_kind = str(getattr(setup, "signal_kind", "") or "").strip()
        estimated_fee_cost_r = self._estimated_fee_cost_r(setup)

        trades = [
            trade
            for trade in (self._to_learning_trade(row) for row in historical_trades)
            if trade is not None and trade.symbol == symbol
        ]
        trades.sort(key=lambda trade: trade.exit_time or datetime.min.replace(tzinfo=UTC), reverse=True)

        symbol_stats = _stats(trade.adjusted_r for trade in trades)
        setup_trades = [trade for trade in trades if trade.signal_kind == signal_kind]
        setup_stats = _stats(trade.adjusted_r for trade in setup_trades)
        recent_churn_count = self._recent_churn_count(trades, current_time)
        recent_loss_streak = self._recent_loss_streak(trades)

        diagnostics = self._diagnostics(
            setup=setup,
            stats=symbol_stats,
            blocked=False,
            reason=None,
            scope=None,
            recent_churn_count=recent_churn_count,
            recent_loss_streak=recent_loss_streak,
            estimated_fee_cost_r=estimated_fee_cost_r,
        )

        if symbol_stats.trade_count >= max(int(self.config.min_symbol_trades), 1) and self._stats_are_poor(symbol_stats):
            return self._blocked(
                setup=setup,
                stats=symbol_stats,
                scope="symbol",
                reason=ENTRY_BLOCKED_LEARNING_SYMBOL_UNDERPERFORMANCE,
                recent_churn_count=recent_churn_count,
                recent_loss_streak=recent_loss_streak,
                estimated_fee_cost_r=estimated_fee_cost_r,
            )

        if signal_kind and setup_stats.trade_count >= max(int(self.config.min_setup_trades), 1) and self._stats_are_poor(setup_stats):
            return self._blocked(
                setup=setup,
                stats=setup_stats,
                scope="setup",
                reason=ENTRY_BLOCKED_LEARNING_SETUP_UNDERPERFORMANCE,
                recent_churn_count=recent_churn_count,
                recent_loss_streak=recent_loss_streak,
                estimated_fee_cost_r=estimated_fee_cost_r,
            )

        if recent_churn_count >= max(int(self.config.recent_churn_min_exits), 1):
            return self._blocked(
                setup=setup,
                stats=symbol_stats,
                scope="recent_churn",
                reason=ENTRY_BLOCKED_LEARNING_RECENT_CHURN,
                recent_churn_count=recent_churn_count,
                recent_loss_streak=recent_loss_streak,
                estimated_fee_cost_r=estimated_fee_cost_r,
            )

        if recent_loss_streak >= max(int(self.config.recent_loss_streak), 1):
            return self._blocked(
                setup=setup,
                stats=symbol_stats,
                scope="loss_streak",
                reason=ENTRY_BLOCKED_LEARNING_LOSS_STREAK,
                recent_churn_count=recent_churn_count,
                recent_loss_streak=recent_loss_streak,
                estimated_fee_cost_r=estimated_fee_cost_r,
            )

        return ExecutorLearningGateDecision(False, None, diagnostics)

    def _blocked(
        self,
        *,
        setup,
        stats: LearningStats,
        scope: str,
        reason: str,
        recent_churn_count: int,
        recent_loss_streak: int,
        estimated_fee_cost_r: float | None,
    ) -> ExecutorLearningGateDecision:
        diagnostics = self._diagnostics(
            setup=setup,
            stats=stats,
            blocked=True,
            reason=reason,
            scope=scope,
            recent_churn_count=recent_churn_count,
            recent_loss_streak=recent_loss_streak,
            estimated_fee_cost_r=estimated_fee_cost_r,
        )
        return ExecutorLearningGateDecision(True, reason, diagnostics)

    def _diagnostics(
        self,
        *,
        setup,
        stats: LearningStats,
        blocked: bool,
        reason: str | None,
        scope: str | None,
        recent_churn_count: int,
        recent_loss_streak: int,
        estimated_fee_cost_r: float | None,
    ) -> dict[str, object]:
        return {
            "learning_gate_enabled": True,
            "entry_blocked_by_learning": bool(blocked),
            "learning_scope": scope,
            "learning_symbol": str(getattr(setup, "symbol", "") or ""),
            "learning_signal_kind": str(getattr(setup, "signal_kind", "") or ""),
            "learning_trade_count": int(stats.trade_count),
            "learning_adjusted_sum_r": _clean_float(stats.adjusted_sum_r),
            "learning_avg_adjusted_r": _clean_float(stats.avg_adjusted_r),
            "learning_profit_factor": _clean_float(stats.adjusted_profit_factor),
            "learning_winrate": _clean_float(stats.adjusted_winrate),
            "learning_recent_churn_count": int(recent_churn_count),
            "learning_recent_loss_streak": int(recent_loss_streak),
            "learning_reason": reason,
            "estimated_roundtrip_cost_pct": _clean_float(self.config.roundtrip_cost_pct),
            "estimated_fee_cost_r": _clean_float(estimated_fee_cost_r),
        }

    def _to_learning_trade(self, row: dict[str, Any]) -> LearningTrade | None:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            return None
        entry_price = _optional_float(row.get("entry_price"))
        initial_sl = _optional_float(row.get("initial_sl"))
        r_result = _optional_float(row.get("r_result"))
        adjusted_r = self.adjusted_r(entry_price=entry_price, initial_sl=initial_sl, r_result=r_result)
        if adjusted_r is None:
            return None
        diagnostics = _parse_diagnostics(row.get("diagnostics_json"))
        signal_kind = str(
            diagnostics.get("signal_kind")
            or diagnostics.get("original_signal_kind")
            or diagnostics.get("confirmed_status")
            or ""
        ).strip()
        return LearningTrade(
            symbol=symbol,
            signal_kind=signal_kind,
            adjusted_r=adjusted_r,
            exit_reason=str(row.get("exit_reason") or "").strip().lower(),
            exit_time=_parse_time(row.get("exit_time")),
        )

    def adjusted_r(self, *, entry_price: float | None, initial_sl: float | None, r_result: float | None) -> float | None:
        if entry_price is None or initial_sl is None or r_result is None:
            return None
        if entry_price <= 0:
            return None
        initial_risk_pct = abs(float(entry_price) - float(initial_sl)) / float(entry_price)
        if initial_risk_pct <= 0:
            return None
        return float(r_result) - (self.config.roundtrip_cost_pct / initial_risk_pct)

    def _estimated_fee_cost_r(self, setup) -> float | None:
        entry_price = _optional_float(getattr(setup, "entry_hint", None))
        initial_sl = _optional_float(getattr(setup, "stop_loss", None))
        if entry_price is None or initial_sl is None or entry_price <= 0:
            return None
        initial_risk_pct = abs(entry_price - initial_sl) / entry_price
        if initial_risk_pct <= 0:
            return None
        return self.config.roundtrip_cost_pct / initial_risk_pct

    def _stats_are_poor(self, stats: LearningStats) -> bool:
        return (
            stats.avg_adjusted_r < float(self.config.min_avg_adjusted_r)
            or stats.adjusted_profit_factor < float(self.config.min_profit_factor)
        )

    def _recent_churn_count(self, trades: list[LearningTrade], now: datetime) -> int:
        cutoff = now - timedelta(minutes=max(float(self.config.recent_churn_lookback_minutes), 0.0))
        return sum(
            1
            for trade in trades
            if trade.exit_time is not None
            and trade.exit_time >= cutoff
            and trade.exit_reason in CHURN_EXIT_REASONS
        )

    def _recent_loss_streak(self, trades: list[LearningTrade]) -> int:
        required = max(int(self.config.recent_loss_streak), 1)
        recent = trades[:required]
        if len(recent) < required:
            return len([trade for trade in recent if trade.adjusted_r <= 0.0])
        if all(trade.adjusted_r <= 0.0 for trade in recent):
            return required
        streak = 0
        for trade in trades:
            if trade.adjusted_r > 0.0:
                break
            streak += 1
        return streak


def _stats(values: Iterable[float]) -> LearningStats:
    adjusted = [float(value) for value in values if math.isfinite(float(value))]
    count = len(adjusted)
    if count <= 0:
        return LearningStats()
    adjusted_sum = sum(adjusted)
    positive = sum(value for value in adjusted if value > 0.0)
    negative = sum(value for value in adjusted if value < 0.0)
    if negative < 0.0:
        profit_factor = positive / abs(negative)
    elif positive > 0.0:
        profit_factor = 999.0
    else:
        profit_factor = 0.0
    wins = sum(1 for value in adjusted if value > 0.0)
    return LearningStats(
        trade_count=count,
        adjusted_sum_r=adjusted_sum,
        avg_adjusted_r=adjusted_sum / count,
        adjusted_profit_factor=profit_factor,
        adjusted_winrate=wins / count,
    )


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_diagnostics(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _parse_time(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clean_float(value: float | None) -> float | None:
    if value is None:
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    return round(value, 8)
