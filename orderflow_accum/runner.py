from __future__ import annotations

from datetime import datetime, timezone

import asyncio
import fnmatch
import json
import logging
import os
import sys
import dataclasses
import sqlite3
import time
from datetime import UTC, datetime
from types import SimpleNamespace

from dashboard.ingest_client import DashboardIngestClient

from .bybit_rest import BybitRestClient, ScanTarget
from .bybit_testnet_executor import BybitTestnetOrderExecutor
from .config import Settings
from .console_ui import ConsoleUI
from .engines import MacroAccumulationEngine, RealtimeAccumulationEngine
from .short_engine import DistributionShortEngine
from .market_regime import MarketRegimeAnalyzer
from .chart_render import render_signal_chart
from .signal_logger import RejectionCsvLogger, SignalCsvLogger
from .signal_store import SignalStore
from .deferred_entry import (
    DeferredEntryStore,
    TRANSIENT_ENTRY_BLOCK_REASONS,
)
from .deferred_entry_service import (
    DeferredEntryCoordinator,
)
from .deferred_entry_runtime import (
    DeferredEntryRuntime,
    DeferredEntryRuntimeConfig,
)
from .deferred_entry_runner_adapter import (
    register_deferred_watch,
)
from .deferred_entry_refresh_service import (
    DeferredEntryRefreshService,
)
from .deferred_entry_snapshot_adapter import (
    build_deferred_entry_snapshot,
)
from .confirmed_promoter import ConfirmedPromoter
from .indicators import add_indicators
from .hybrid_entry_shadow import HybridEntryShadowEngine
from .executor_exit_shadow import (
    DEFAULT_EXIT_SHADOW_POLICY,
    current_unrealized_r,
    evaluate_exit_shadow_policy,
    utc_now_iso,
)
from .executor_learning_gate import ExecutorLearningGate, ExecutorLearningGateConfig
from .telegram_notify import TelegramNotifier
from .trade_learning import TradeLearningEngine
from .trade_executor import (
    ENTERED,
    ENTER_LONG,
    EXIT,
    ENTER_SHORT,
    MOVE_SL_TO_BREAKEVEN,
    PROTECT_BREAKEVEN,
    TRAILING_PROFIT,
    WATCH,
    ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION,
    OrderflowSnapshot,
    SmartTradeExecutor,
    MANAGEMENT_POLICY_LEGACY,
    TradeDecision,
    TradePosition,
    TradeSetup,
)
from .ws_clients import MarketStream


class AccumulationRunner:
    VOLUME_IMPULSE_REPORT_CAP = 50.0
    ACTIVE_R_SUSPICIOUS_THRESHOLD = 25.0
    TERMINAL_EXECUTOR_OUTCOME_STATES = {"EXITED", "CLOSED"}
    TERMINAL_EXECUTOR_OUTCOME_ACTIONS = {"STALE_RESET", "PHANTOM_RESET"}
    ACTIVE_EXECUTOR_OUTCOME_STATES = {ENTERED, PROTECT_BREAKEVEN, TRAILING_PROFIT}
    TESTNET_OBSERVATION_ENTRY_KINDS = {
        "PRE_IMPULSE_ZONE",
        "BREAKOUT_PRESSURE",
        "ACCUMULATION_LONG_READY",
        "ABSORPTION_ZONE",
        "BASE_BUILDUP_LONG",
    }
    TESTNET_OBSERVATION_ENTRY_STATUSES = {
        "PRE_IMPULSE",
        "BREAKOUT_PRESSURE",
        "PENDING",
        "ACCUMULATION",
        "WATCHING",
        "CONFIRMED_LONG",
    }
    TESTNET_OBSERVATION_BLOCKED_BTC_REGIMES = {"BTC_BEARISH", "BTC_DUMP_RISK"}
    MISSED_SIGNAL_MEMORY_KINDS = {"BREAKOUT_PRESSURE", "PRE_IMPULSE_ZONE", "ABSORPTION_ZONE"}
    LATE_CHASE_EXHAUSTION_REASON = "entry_blocked_late_impulse_exhaustion"
    LATE_CHASE_DISTANCE_REASON = "entry_blocked_late_chase_distance"
    LATE_CHASE_MISSED_MOVE_REASON = "entry_blocked_late_chase_missed_move"
    DISTRIBUTION_AFTER_IMPULSE_REASON = "entry_blocked_distribution_after_impulse"
    DISTRIBUTION_RISK_AFTER_IMPULSE_REASON = "entry_blocked_distribution_risk_after_impulse"

    def __init__(self, settings: Settings, ui: ConsoleUI | None = None, version: str = "ACCUM V1.4.2 DIAG"):
        self.settings = settings
        self.ui = ui or ConsoleUI()
        self.version = version
        self.logger = logging.getLogger("Accum.Runner")
        self.macro_logger = logging.getLogger("Accum.Signal.Macro")
        self.orderflow_logger = logging.getLogger("Accum.Signal.Realtime")
        self.telegram = TelegramNotifier(settings.telegram_token, settings.telegram_chat_id)
        self.realtime_engine = RealtimeAccumulationEngine(settings)
        self.short_engine = DistributionShortEngine(settings)
        self.regime_analyzer = MarketRegimeAnalyzer(
            short_bonus=settings.short_btc_bonus,
            long_bearish_penalty=settings.long_btc_bearish_penalty,
        )
        self.macro_engine = MacroAccumulationEngine(settings)
        self.csv_logger = SignalCsvLogger("accumulation_signals.csv")
        self.rejection_logger = RejectionCsvLogger("rejection_reasons.csv")
        self.signal_store = SignalStore()

        # Deferred entry must have zero DB/schema side effects while disabled.
        # The feature stays paper-only and off by default.
        deferred_entry_enabled = self._env_bool(
            "EXECUTOR_DEFERRED_ENTRY_ENABLED",
            False,
        )
        self.deferred_entry_store = None
        self.deferred_entry_runtime = None
        self.deferred_entry_refresh_service = None
        self._deferred_entry_structure_cache: dict[
            tuple[str, str],
            tuple[float, dict[str, object]],
        ] = {}

        if deferred_entry_enabled:
            self.deferred_entry_store = DeferredEntryStore(
                str(self.signal_store.db_path)
            )
            coordinator = DeferredEntryCoordinator(
                self.deferred_entry_store
            )
            self.deferred_entry_runtime = DeferredEntryRuntime(
                coordinator,
                config=DeferredEntryRuntimeConfig(
                    enabled=True,
                    ttl_hours=self._env_float(
                        "EXECUTOR_DEFERRED_ENTRY_TTL_HOURS",
                        24.0,
                    ),
                    h1_only=self._env_bool(
                        "EXECUTOR_DEFERRED_ENTRY_H1_ONLY",
                        True,
                    ),
                    early_statuses=self._env_upper_csv(
                        "EXECUTOR_DEFERRED_ENTRY_ALLOWED_STATUSES",
                        (
                            "PRE_IMPULSE",
                            "BREAKOUT_PRESSURE",
                            "PENDING",
                        ),
                    ),
                    early_kinds=self._env_upper_csv(
                        "EXECUTOR_DEFERRED_ENTRY_ALLOWED_KINDS",
                        (
                            "PRE_IMPULSE_ZONE",
                            "BREAKOUT_PRESSURE",
                            "ACCUMULATION_LONG_READY",
                        ),
                    ),
                    min_early_score=max(
                        self._env_float(
                            "EXECUTOR_DEFERRED_ENTRY_MIN_SCORE",
                            10.0,
                        ),
                        0.0,
                    ),
                    blocked_btc_regimes=self._env_upper_csv(
                        "EXECUTOR_DEFERRED_ENTRY_BLOCKED_BTC_REGIMES",
                        (
                            "BTC_BEARISH",
                            "BTC_DUMP_RISK",
                        ),
                    ),
                ),
            )

        if deferred_entry_enabled:
            self.deferred_entry_refresh_service = (
                DeferredEntryRefreshService(
                    coordinator,
                    max_active=max(
                        1,
                        min(
                            int(
                                self._env_float(
                                    "EXECUTOR_DEFERRED_ENTRY_REFRESH_MAX_ACTIVE",
                                    12.0,
                                )
                            ),
                            200,
                        ),
                    ),
                )
            )

        self.trade_executor_mode = self._resolve_trade_executor_mode(settings)
        self.trade_executor_enabled = (
            (os.getenv("RUN_TRADE_EXECUTOR", "false").strip().lower() == "true" and self.trade_executor_mode == "paper")
            or self.trade_executor_mode == "testnet"
        )
        self.trade_executor = self._build_trade_executor() if self.trade_executor_enabled else None
        self.testnet_order_executor = (
            BybitTestnetOrderExecutor(self.signal_store, notifier=self.telegram, logger_=self.logger)
            if self.trade_executor_mode == "testnet"
            else None
        )
        self.logger.info(
            "Bybit routing configured | market_data_testnet=%s | order_testnet=%s | trade_executor_mode=%s",
            self.settings.bybit_market_data_testnet,
            self.settings.bybit_testnet,
            self.trade_executor_mode,
        )
        self.hybrid_entry_shadow = HybridEntryShadowEngine(
            min_volume_impulse=self._env_float("HYBRID_ENTRY_SHADOW_MIN_VOLUME_IMPULSE", 1.2),
            max_spread_bps=self._env_float("HYBRID_ENTRY_SHADOW_MAX_SPREAD_BPS", 15.0),
            ask_wall_entry_limit=self._env_float("HYBRID_ENTRY_SHADOW_ASK_WALL_LIMIT", 0.65),
        )
        self.executor_exit_shadow_enabled = os.getenv("EXECUTOR_EXIT_SHADOW_ENABLED", "false").strip().lower() == "true"
        self.executor_exit_shadow_policy = os.getenv("EXECUTOR_EXIT_SHADOW_POLICY", DEFAULT_EXIT_SHADOW_POLICY).strip() or DEFAULT_EXIT_SHADOW_POLICY
        self.executor_learning_gate = ExecutorLearningGate(ExecutorLearningGateConfig.from_env())
        self.trade_learning = TradeLearningEngine(self.signal_store, logger=self.logger)
        self.dashboard = DashboardIngestClient()
        self.promoter = ConfirmedPromoter()
        self._cooldowns: dict[str, float] = {}
        self._counts = {"macro": 0, "orderflow": 0}

        # H1 is analyzed only after the candle closes, and each closed H1 bar
        # is evaluated once per symbol. This changes timing only, not setup logic.
        self._processed_closed_h1_bars: dict[tuple[str, str], str] = {}
        self._preimpulse_kinds = {
            "ACCUMULATION_WATCH",
            "ABSORPTION_ZONE",
            "PRE_IMPULSE_ZONE",
            "BREAKOUT_PRESSURE",
            "SHORT_WATCH",
            "DISTRIBUTION_ZONE",
            "PRE_DUMP_ZONE",
            "CONFIRMED_BREAKDOWN",
        }

    @staticmethod
    def _normalize_trade_executor_mode(value: object | None) -> str:
        mode = str(value or "paper").strip().lower()
        return mode or "paper"

    @classmethod
    def _resolve_trade_executor_mode(cls, settings: Settings) -> str:
        configured_mode = getattr(
            settings,
            "trade_executor_mode",
            os.getenv("TRADE_EXECUTOR_MODE", "paper"),
        )
        return cls._normalize_trade_executor_mode(configured_mode)

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_float(name: str, default: float) -> float:
        value = os.getenv(name)
        if value is None or not value.strip():
            return default
        try:
            return float(value)
        except ValueError:
            return default

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        value = os.getenv(name)
        if value is None or not value.strip():
            return default
        try:
            return int(float(value))
        except ValueError:
            return default

    @staticmethod
    def _env_upper_csv(
        name: str,
        default: tuple[str, ...],
    ) -> tuple[str, ...]:
        raw = os.getenv(name)
        source = raw if raw is not None else ",".join(default)
        values = tuple(
            item.strip().upper()
            for item in source.split(",")
            if item.strip()
        )
        return values or default

    def _resolve_executor_management_policy(self) -> str:
        configured_policy = getattr(self.settings, "executor_management_policy", None)
        if configured_policy is None or not str(configured_policy).strip():
            configured_policy = os.getenv("EXECUTOR_MANAGEMENT_POLICY", MANAGEMENT_POLICY_LEGACY)
        policy = str(configured_policy or MANAGEMENT_POLICY_LEGACY).strip().lower()
        return policy or MANAGEMENT_POLICY_LEGACY

    def _build_trade_executor(self) -> SmartTradeExecutor:
        return SmartTradeExecutor(
            trade_executor_mode=self.trade_executor_mode,
            management_policy=self._resolve_executor_management_policy(),
            protect_after_1r=self._env_bool("EXECUTOR_PROTECT_AFTER_1R", False),
            min_protected_r_after_1r=self._env_float("EXECUTOR_MIN_PROTECTED_R_AFTER_1R", 0.25),
            soft_orderflow_exit_min_r=self._env_float("EXECUTOR_SOFT_ORDERFLOW_EXIT_MIN_R", 0.25),
            soft_orderflow_exit_min_bars=self._env_int("EXECUTOR_SOFT_ORDERFLOW_EXIT_MIN_BARS", 2),
            structural_trailing_enabled=self._env_bool("EXECUTOR_STRUCTURAL_TRAILING_ENABLED", True),
            structural_trailing_start_r=self._env_float("EXECUTOR_STRUCTURAL_TRAILING_START_R", 0.5),
            structural_trailing_buffer_bps=self._env_float("EXECUTOR_STRUCTURAL_TRAILING_BUFFER_BPS", 15.0),
            structural_trailing_min_lock_r=self._env_float("EXECUTOR_STRUCTURAL_TRAILING_MIN_LOCK_R", 0.25),
            nearest_structure_sl_enabled=self._env_bool("EXECUTOR_NEAREST_STRUCTURE_SL_ENABLED", True),
        )

    def _executor_learning_gate(self) -> ExecutorLearningGate:
        gate = getattr(self, "executor_learning_gate", None)
        if gate is None:
            gate = ExecutorLearningGate(ExecutorLearningGateConfig.from_env())
            self.executor_learning_gate = gate
        return gate

    def _evaluate_executor_learning_gate(self, setup: TradeSetup) -> tuple[TradeDecision | None, dict[str, object]]:
        gate = self._executor_learning_gate()
        if not gate.config.enabled:
            return None, {}
        try:
            trades = self.signal_store.list_recent_closed_executor_trades(gate.config.lookback_hours)
            learning_decision = gate.evaluate(setup, trades)
        except Exception:
            self.logger.exception("Executor learning gate failed for %s", setup.symbol)
            return None, {}
        if not learning_decision.blocked:
            return None, learning_decision.diagnostics
        return (
            TradeDecision(WATCH, str(learning_decision.reason), "TRADE_WATCH", None),
            learning_decision.diagnostics,
        )

    def _signal_forward_tracking_enabled(self) -> bool:
        return self._env_bool("SIGNAL_FORWARD_TRACKING_ENABLED", True)

    def _signal_forward_lookback_hours(self) -> float:
        return max(self._env_float("SIGNAL_FORWARD_TRACKING_HOURS", 72.0), 0.0)

    def _signal_forward_targets_pct(self) -> list[float]:
        raw = os.getenv("SIGNAL_FORWARD_TARGETS_PCT", "3,5,10,15")
        targets: list[float] = []
        for item in str(raw or "").split(","):
            try:
                value = float(item.strip())
            except ValueError:
                continue
            if value > 0:
                targets.append(value)
        return targets or [3.0, 5.0, 10.0, 15.0]

    def _early_breakout_entry_enabled(self) -> bool:
        return self._env_bool("EXECUTOR_EARLY_BREAKOUT_ENTRY", True)

    def _late_chase_gate_enabled(self) -> bool:
        return self._env_bool("EXECUTOR_LATE_CHASE_GATE", True)

    def _late_chase_lookback_hours(self) -> float:
        minutes = max(self._env_float("EXECUTOR_LATE_CHASE_LOOKBACK_MINUTES", 240.0), 0.0)
        return minutes / 60.0

    @classmethod
    def _is_missed_signal_memory_kind(cls, kind: object) -> bool:
        return str(kind or "").strip().upper() in cls.MISSED_SIGNAL_MEMORY_KINDS

    def _forward_signal_trackable(self, signal) -> bool:
        if not self._signal_forward_tracking_enabled():
            return False
        if str(getattr(signal, "side", "") or "") != "Buy":
            return False
        score = self._optional_float(getattr(signal, "score", None)) or 0.0
        if score < 8.0:
            return False
        return True

    @staticmethod
    def _first_present_mapping_value(mapping: dict[str, object], keys: tuple[str, ...]):
        for key in keys:
            if key in mapping and mapping.get(key) not in (None, ""):
                return mapping.get(key)
        return None

    def _meta_float(self, meta: dict[str, object], snapshot: OrderflowSnapshot | None, keys: tuple[str, ...]) -> float | None:
        override = meta.get("executor_snapshot")
        if isinstance(override, dict):
            parsed = self._optional_float(self._first_present_mapping_value(override, keys))
            if parsed is not None:
                return parsed
        parsed = self._optional_float(self._first_present_mapping_value(meta, keys))
        if parsed is not None:
            return parsed
        if snapshot is not None:
            for key in keys:
                parsed = self._optional_float(getattr(snapshot, key, None))
                if parsed is not None:
                    return parsed
        return None

    def _forward_signal_price(self, signal) -> float | None:
        meta = dict(getattr(signal, "meta", {}) or {})
        return self._optional_float(
            self._first_present_mapping_value(meta, ("original_signal_price", "signal_price", "scanner_entry"))
        ) or self._optional_float(getattr(signal, "entry", None))

    def _forward_range_high(self, signal, snapshot: OrderflowSnapshot | None) -> float | None:
        meta = dict(getattr(signal, "meta", {}) or {})
        return self._meta_float(meta, snapshot, ("range_high", "corridor_high", "resistance", "resistance_1"))

    def _forward_range_low(self, signal, snapshot: OrderflowSnapshot | None) -> float | None:
        meta = dict(getattr(signal, "meta", {}) or {})
        return (
            self._meta_float(meta, snapshot, ("range_low", "corridor_low", "support"))
            or self._optional_float(getattr(signal, "stop_loss", None))
        )

    def _forward_invalidation_price(self, signal, snapshot: OrderflowSnapshot | None) -> float | None:
        meta = dict(getattr(signal, "meta", {}) or {})
        return (
            self._meta_float(meta, snapshot, ("invalidation_price", "invalid_price", "invalidation"))
            or self._forward_range_low(signal, snapshot)
            or self._optional_float(getattr(signal, "stop_loss", None))
        )

    def _record_signal_forward_outcome(
        self,
        signal_key: str,
        signal,
        *,
        status: str | None,
        snapshot: OrderflowSnapshot | None,
        executor_block_reason: str | None,
    ) -> dict[str, object] | None:
        if not self._forward_signal_trackable(signal):
            return None
        current_price = self._optional_float(getattr(snapshot, "price", None)) if snapshot is not None else None
        signal_price = self._forward_signal_price(signal)
        if current_price is None or current_price <= 0 or signal_price is None or signal_price <= 0:
            return None
        try:
            return self.signal_store.upsert_signal_forward_outcome(
                signal_key=signal_key,
                symbol=str(signal.symbol),
                timeframe=str(getattr(signal, "meta", {}).get("tf") or "1"),
                kind=str(getattr(signal, "kind", "") or ""),
                status=status,
                side=str(getattr(signal, "side", "") or ""),
                signal_price=signal_price,
                current_price=current_price,
                range_high=self._forward_range_high(signal, snapshot),
                range_low=self._forward_range_low(signal, snapshot),
                invalidation_price=self._forward_invalidation_price(signal, snapshot),
                executor_block_reason=executor_block_reason,
            )
        except Exception:
            self.logger.exception("Signal forward outcome tracking failed for %s", signal_key)
            return None

    def _recent_forward_outcomes_for_setup(
        self,
        setup: TradeSetup,
        *,
        include_kind: bool = False,
        lookback_hours: float | None = None,
    ) -> list[dict[str, object]]:
        if not self._signal_forward_tracking_enabled():
            return []
        try:
            return self.signal_store.list_recent_signal_forward_outcomes(
                symbol=setup.symbol,
                kind=setup.signal_kind if include_kind else None,
                timeframe=setup.timeframe,
                lookback_hours=self._signal_forward_lookback_hours() if lookback_hours is None else lookback_hours,
            )
        except Exception:
            self.logger.exception("Missed signal memory lookup failed for %s", setup.symbol)
            return []

    def _similar_forward_outcomes(self, setup: TradeSetup) -> list[dict[str, object]]:
        rows = self._recent_forward_outcomes_for_setup(setup, include_kind=False)
        setup_kind = str(setup.signal_kind or "").strip().upper()
        similar: list[dict[str, object]] = []
        for row in rows:
            row_kind = str(row.get("kind") or "").strip().upper()
            if row_kind == setup_kind or row_kind in self.MISSED_SIGNAL_MEMORY_KINDS:
                similar.append(row)
        return similar

    def _missed_signal_memory_diagnostics(self, setup: TradeSetup) -> dict[str, object]:
        if not self._signal_forward_tracking_enabled():
            return {}
        similar = self._similar_forward_outcomes(setup)
        gains = [
            float(gain)
            for row in similar
            if (gain := self._optional_float(row.get("max_forward_gain_pct"))) is not None
        ]
        positive_threshold = max(5.0, min(self._signal_forward_targets_pct() or [5.0]))
        positive = [
            row
            for row in similar
            if (self._optional_float(row.get("max_forward_gain_pct")) or 0.0) >= positive_threshold
            and not bool(row.get("hit_invalidation"))
        ]
        reasons: dict[str, int] = {}
        for row in similar:
            reason = str(row.get("executor_block_reason") or "").strip()
            if reason:
                reasons[reason] = reasons.get(reason, 0) + 1
        common_reasons = [
            reason for reason, _count in sorted(reasons.items(), key=lambda item: (-item[1], item[0]))[:3]
        ]
        return {
            "missed_signal_memory_enabled": True,
            "missed_signal_similar_count": len(similar),
            "missed_signal_positive_count": len(positive),
            "missed_signal_avg_forward_gain_pct": (sum(gains) / len(gains)) if gains else None,
            "missed_signal_best_forward_gain_pct": max(gains) if gains else None,
            "missed_signal_common_block_reasons": common_reasons,
        }

    def _best_forward_outcome_for_entry(self, signal_key: str, setup: TradeSetup) -> dict[str, object] | None:
        exact = None
        try:
            row = self.signal_store.get_signal_forward_outcome(signal_key)
            exact = dict(row) if row is not None else None
        except Exception:
            exact = None
        if exact is not None:
            return exact
        rows = self._recent_forward_outcomes_for_setup(setup, include_kind=False, lookback_hours=self._late_chase_lookback_hours())
        setup_kind = str(setup.signal_kind or "").strip().upper()
        candidates = [
            row
            for row in rows
            if str(row.get("kind") or "").strip().upper() == setup_kind
            or str(row.get("kind") or "").strip().upper() in self.MISSED_SIGNAL_MEMORY_KINDS
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: self._optional_float(row.get("max_forward_gain_pct")) or 0.0)

    @staticmethod
    def _bps_above(price: float | None, reference: float | None) -> float | None:
        if price is None or reference is None or reference <= 0:
            return None
        return ((price - reference) / reference) * 10000.0

    def _open_executor_position(self, setup: TradeSetup, snapshot: OrderflowSnapshot, *, force: bool = False) -> TradePosition:
        if not force:
            return self.trade_executor.open_position(setup, snapshot)
        entry_price = float(snapshot.price)
        initial_sl = self.trade_executor._initial_stop_loss(setup, snapshot)
        initial_risk = self.trade_executor._calculate_initial_risk(setup.side, entry_price, initial_sl)
        return TradePosition(
            symbol=setup.symbol,
            side=setup.side,
            state=ENTERED,
            entry_price=entry_price,
            stop_loss=initial_sl,
            current_sl=initial_sl,
            max_price=entry_price,
            min_price=entry_price,
            max_gain_r=0.0,
            max_drawdown_r=0.0,
            bars_in_trade=0,
            exit_price=None,
            exit_reason=None,
            initial_risk=initial_risk,
        )

    def _evaluate_early_breakout_entry(
        self,
        signal_key: str,
        signal,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
    ) -> tuple[TradeDecision | None, dict[str, object]]:
        if not self._early_breakout_entry_enabled():
            return None, {}
        kind = str(setup.signal_kind or "").strip().upper()
        if setup.side != "Buy" or kind not in self.MISSED_SIGNAL_MEMORY_KINDS:
            return None, {}
        range_high = self._forward_range_high(signal, snapshot)
        max_retest_bps = max(self._env_float("EXECUTOR_EARLY_BREAKOUT_MAX_RETEST_BPS", 35.0), 0.0)
        min_hold_bars = max(self._env_int("EXECUTOR_EARLY_BREAKOUT_MIN_HOLD_BARS", 1), 0)
        require_volume_not_dead = self._env_bool("EXECUTOR_EARLY_BREAKOUT_REQUIRE_VOLUME_NOT_DEAD", True)
        distance_from_range_high = self._bps_above(snapshot.price, range_high)
        breakout = range_high is not None and snapshot.price > range_high
        retest_hold = (
            range_high is not None
            and distance_from_range_high is not None
            and abs(distance_from_range_high) <= max_retest_bps
            and int(snapshot.bars_since_entry or 0) >= min_hold_bars
            and snapshot.price >= range_high * (1.0 - max_retest_bps / 10000.0)
        )
        volume_not_dead = snapshot.volume_impulse >= 0.75
        sell_pressure_absorbed = snapshot.buy_flow >= snapshot.sell_flow * 0.95 and snapshot.sell_flow <= snapshot.buy_flow * 1.15
        spread_ok = snapshot.spread_bps <= float(getattr(self.trade_executor, "max_spread_bps", 15.0))
        ask_wall_ok = snapshot.ask_wall_strength <= float(getattr(self.trade_executor, "ask_wall_entry_limit", 0.65))
        btc_ok = setup.btc_regime not in self.TESTNET_OBSERVATION_BLOCKED_BTC_REGIMES
        score_ok = setup.score >= float(getattr(self.trade_executor, "min_long_score", 8.0))
        allowed = (
            range_high is not None
            and score_ok
            and btc_ok
            and spread_ok
            and ask_wall_ok
            and sell_pressure_absorbed
            and (volume_not_dead or not require_volume_not_dead)
            and (breakout or retest_hold)
        )
        diagnostics = {
            "early_breakout_entry_enabled": True,
            "early_breakout_entry_allowed": bool(allowed),
            "early_breakout_signal_key": signal_key,
            "early_breakout_signal_kind": kind,
            "early_breakout_reason": "entry_allowed_early_breakout_retest" if allowed else None,
            "original_range_high": range_high,
            "early_breakout_break_above_range": bool(breakout),
            "early_breakout_retest_hold": bool(retest_hold),
            "early_breakout_distance_from_range_high_bps": distance_from_range_high,
            "early_breakout_volume_not_dead": bool(volume_not_dead),
            "early_breakout_sell_pressure_absorbed": bool(sell_pressure_absorbed),
            "early_breakout_spread_ok": bool(spread_ok),
            "early_breakout_btc_ok": bool(btc_ok),
        }
        if not allowed:
            return None, diagnostics
        return TradeDecision(ENTER_LONG, "entry_allowed_early_breakout_retest", ENTERED, None), diagnostics

    def _meta_bool(self, meta: dict[str, object], keys: tuple[str, ...]) -> bool:
        override = meta.get("executor_snapshot")
        sources = [meta]
        if isinstance(override, dict):
            sources.insert(0, override)
        for source in sources:
            for key in keys:
                value = source.get(key)
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return bool(value)
                if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}:
                    return True
        return False

    def _distribution_after_impulse_diagnostics(
        self,
        signal,
        snapshot: OrderflowSnapshot,
        *,
        original_range_high: float | None,
        missed_forward_gain_pct: float | None,
    ) -> dict[str, object]:
        meta = dict(getattr(signal, "meta", {}) or {})
        previous_buy = self._meta_float(meta, snapshot, ("previous_buy_flow", "buy_flow_prev", "buy_flow_peak"))
        previous_sell = self._meta_float(meta, snapshot, ("previous_sell_flow", "sell_flow_prev"))
        buy_flow_decelerating = self._meta_bool(meta, ("buy_flow_decelerating", "buy_flow_declining"))
        if not buy_flow_decelerating and previous_buy is not None and previous_buy > 0:
            buy_flow_decelerating = snapshot.buy_flow < previous_buy * 0.95
        sell_flow_increasing = self._meta_bool(meta, ("sell_flow_increasing", "sell_flow_rising"))
        if not sell_flow_increasing and previous_sell is not None and previous_sell > 0:
            sell_flow_increasing = snapshot.sell_flow > previous_sell * 1.05
        if not sell_flow_increasing:
            sell_flow_increasing = snapshot.sell_flow >= snapshot.buy_flow * 0.9

        high_volume_low_displacement = self._meta_bool(
            meta,
            ("high_volume_low_displacement_after_impulse", "high_volume_low_displacement"),
        )
        upper_wick_ratio = self._meta_float(meta, snapshot, ("upper_wick_ratio", "recent_upper_wick_ratio"))
        upper_wick_ratio_high = self._meta_bool(meta, ("upper_wick_ratio_high",)) or (
            upper_wick_ratio is not None and upper_wick_ratio >= 0.55
        )
        failed_hold = self._meta_bool(meta, ("failed_hold_above_range_high", "failed_breakout_hold"))
        if not failed_hold and original_range_high is not None:
            failed_hold = snapshot.price < original_range_high and (missed_forward_gain_pct or 0.0) >= 3.0
        retests = self._meta_float(meta, snapshot, ("resistance_retests_after_impulse", "repeated_resistance_retests", "resistance_retests"))
        repeated_retests = self._meta_bool(meta, ("repeated_resistance_retests",)) or (
            retests is not None and retests >= 2
        )
        sell_flow_dominance = self._meta_bool(meta, ("sell_flow_dominance_after_breakout",)) or (
            snapshot.sell_flow >= snapshot.buy_flow * 1.05
        )
        risk_votes = sum(
            int(flag)
            for flag in (
                high_volume_low_displacement,
                sell_flow_dominance,
                upper_wick_ratio_high,
                failed_hold,
                repeated_retests and buy_flow_decelerating,
            )
        )
        return {
            "buy_flow_decelerating": bool(buy_flow_decelerating),
            "sell_flow_increasing": bool(sell_flow_increasing),
            "high_volume_low_displacement_after_impulse": bool(high_volume_low_displacement),
            "upper_wick_ratio_high": bool(upper_wick_ratio_high),
            "failed_hold_above_range_high": bool(failed_hold),
            "repeated_resistance_retests_after_impulse": bool(repeated_retests),
            "sell_flow_dominance_after_breakout": bool(sell_flow_dominance),
            "distribution_risk_after_impulse_votes": risk_votes,
        }

    def _late_chase_context(
        self,
        signal_key: str,
        signal,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
    ) -> tuple[dict[str, object], bool]:
        meta = dict(getattr(signal, "meta", {}) or {})
        forward_row = self._best_forward_outcome_for_entry(signal_key, setup)
        applicable = bool(forward_row) or self._is_missed_signal_memory_kind(setup.signal_kind) or any(
            key in meta for key in ("original_signal_price", "signal_price", "range_high", "corridor_high")
        )
        if not applicable:
            return {}, False
        original_signal_price = (
            self._optional_float(forward_row.get("signal_price")) if forward_row is not None else None
        ) or self._forward_signal_price(signal)
        original_range_high = (
            self._optional_float(forward_row.get("range_high")) if forward_row is not None else None
        ) or self._forward_range_high(signal, snapshot)
        missed_forward_gain_pct = (
            self._optional_float(forward_row.get("max_forward_gain_pct")) if forward_row is not None else None
        )
        distance_from_signal = self._bps_above(snapshot.price, original_signal_price)
        distance_from_range_high = self._bps_above(snapshot.price, original_range_high)
        diagnostics = {
            "late_chase_gate_enabled": True,
            "original_signal_price": original_signal_price,
            "original_range_high": original_range_high,
            "distance_from_signal_bps": distance_from_signal,
            "distance_from_range_high_bps": distance_from_range_high,
            "missed_forward_gain_pct": missed_forward_gain_pct,
            "entry_blocked_late_chase": False,
            "late_chase_reason": None,
        }
        diagnostics.update(
            self._distribution_after_impulse_diagnostics(
                signal,
                snapshot,
                original_range_high=original_range_high,
                missed_forward_gain_pct=missed_forward_gain_pct,
            )
        )
        return diagnostics, True

    def _evaluate_late_chase_gate(
        self,
        signal_key: str,
        signal,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
    ) -> tuple[TradeDecision | None, dict[str, object]]:
        if not self._late_chase_gate_enabled() or setup.side != "Buy":
            return None, {}
        diagnostics, applicable = self._late_chase_context(signal_key, signal, setup, snapshot)
        if not applicable:
            return None, {}
        max_signal_bps = max(self._env_float("EXECUTOR_MAX_ENTRY_DISTANCE_FROM_SIGNAL_BPS", 120.0), 0.0)
        max_range_bps = max(self._env_float("EXECUTOR_MAX_ENTRY_DISTANCE_FROM_RANGE_HIGH_BPS", 60.0), 0.0)
        max_missed_move = max(self._env_float("EXECUTOR_MAX_MISSED_MOVE_BEFORE_ENTRY_PCT", 5.0), 0.0)
        distance_from_signal = self._optional_float(diagnostics.get("distance_from_signal_bps"))
        distance_from_range = self._optional_float(diagnostics.get("distance_from_range_high_bps"))
        missed_gain = self._optional_float(diagnostics.get("missed_forward_gain_pct"))

        reason = None
        if (
            (distance_from_signal is not None and distance_from_signal > max_signal_bps)
            or (distance_from_range is not None and distance_from_range > max_range_bps)
        ):
            reason = self.LATE_CHASE_DISTANCE_REASON
        elif missed_gain is not None and missed_gain >= max_missed_move:
            reason = self.LATE_CHASE_MISSED_MOVE_REASON
        elif (
            (missed_gain or 0.0) >= 3.0
            and diagnostics.get("buy_flow_decelerating")
            and diagnostics.get("sell_flow_increasing")
        ):
            reason = self.LATE_CHASE_EXHAUSTION_REASON
        elif diagnostics.get("high_volume_low_displacement_after_impulse") and diagnostics.get("sell_flow_increasing"):
            reason = self.DISTRIBUTION_AFTER_IMPULSE_REASON
        elif int(diagnostics.get("distribution_risk_after_impulse_votes") or 0) >= 2:
            reason = self.DISTRIBUTION_RISK_AFTER_IMPULSE_REASON

        if reason is None:
            return None, diagnostics
        diagnostics["entry_blocked_late_chase"] = True
        diagnostics["late_chase_reason"] = reason
        return TradeDecision(WATCH, reason, "TRADE_WATCH", None), diagnostics

    def _filter_symbols(self, symbols: list[ScanTarget]) -> list[ScanTarget]:
        out: list[ScanTarget] = []
        seen: set[tuple[str, str]] = set()

        for target in symbols:
            symbol = target.symbol

            if any(fnmatch.fnmatch(symbol, pattern) for pattern in self.settings.symbol_exclude_patterns):
                continue

            if symbol in self.settings.symbols_blocklist:
                continue

            key = (symbol, target.market)

            if key in seen:
                continue

            seen.add(key)
            out.append(target)

        return out

    def _watchlist_realtime_enabled(self) -> bool:
        return self._env_bool("WATCHLIST_REALTIME_ENABLED", False)

    def _watchlist_trade_eligible_only(self) -> bool:
        return self._env_bool("WATCHLIST_USE_TRADE_ELIGIBLE_ONLY", True)

    def _watchlist_db_path(self) -> str:
        return os.getenv("SIGNALS_DB_PATH", "data/signals.db").strip() or "data/signals.db"

    def _watchlist_realtime_observe_phases(self) -> set[str]:
        raw = os.getenv("WATCHLIST_REALTIME_OBSERVE_PHASES", "MOMENTUM_OBSERVE")
        return {
            item.strip().upper()
            for item in raw.split(",")
            if item and item.strip()
        }

    def _load_market_watchlist_targets(
        self,
        *,
        limit: int,
        trade_eligible_only: bool | None = None,
        include_observe_phases: set[str] | None = None,
    ) -> list[ScanTarget]:
        db_path = self._watchlist_db_path()
        if not os.path.exists(db_path):
            return []

        if trade_eligible_only is None:
            trade_eligible_only = self._watchlist_trade_eligible_only()

        observe_phases = {
            str(phase).strip().upper()
            for phase in (include_observe_phases or set())
            if str(phase).strip()
        }

        where = [
            "active=1",
            "(expires_at IS NULL OR expires_at='' OR expires_at >= ?)",
        ]
        params: list[object] = [datetime.now(timezone.utc).isoformat()]

        if trade_eligible_only:
            where.append("trade_eligible=1")
        elif observe_phases:
            placeholders = ",".join("?" for _ in observe_phases)
            where.append(f"(trade_eligible=1 OR UPPER(phase) IN ({placeholders}))")
            params.extend(sorted(observe_phases))
        else:
            where.append("trade_eligible=1")

        sql = f"""
            SELECT symbol, market
            FROM market_watchlist
            WHERE {' AND '.join(where)}
            ORDER BY trade_eligible DESC, score DESC, last_seen_at DESC
            LIMIT ?
        """
        params.append(max(int(limit), 1))

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
            conn.close()
        except sqlite3.Error as exc:
            self.logger.warning("Market watchlist load failed: %r", exc)
            return []

        targets = [
            ScanTarget(
                symbol=str(row["symbol"]).upper(),
                market=str(row["market"] or "linear").lower(),
            )
            for row in rows
            if row["symbol"]
        ]
        return self._filter_symbols(targets)

    def _replace_scan_targets(self, current: list[ScanTarget], updated: list[ScanTarget], *, label: str) -> bool:
        if not updated:
            return False

        current_keys = [(target.symbol, target.market) for target in current]
        updated_keys = [(target.symbol, target.market) for target in updated]

        if current_keys == updated_keys:
            return False

        current[:] = updated
        self.logger.info("Market watchlist replaced %s symbols: %s", label, len(updated))
        return True

    def _watchlist_initial_targets(
        self,
        fallback: list[ScanTarget],
        *,
        limit: int,
        label: str,
    ) -> list[ScanTarget]:
        if not self._watchlist_realtime_enabled():
            return fallback

        if label == "realtime":
            targets = self._load_market_watchlist_targets(
                limit=limit,
                trade_eligible_only=False,
                include_observe_phases=self._watchlist_realtime_observe_phases(),
            )
        else:
            targets = self._load_market_watchlist_targets(
                limit=limit,
                trade_eligible_only=True,
            )

        if not targets:
            self.logger.warning(
                "Market watchlist enabled but empty for %s; using fallback symbols=%s",
                label,
                len(fallback),
            )
            return fallback

        self.logger.info(
            "Market watchlist enabled for %s: using %s symbols",
            label,
            len(targets),
        )
        return targets

    async def _refresh_realtime_symbols_from_watchlist(self, stream: MarketStream, symbols: list[ScanTarget]) -> None:
        if not self._watchlist_realtime_enabled():
            return

        interval = max(self._env_float("WATCHLIST_REALTIME_REFRESH_SECONDS", 300.0), 30.0)
        now = time.monotonic()
        last = float(getattr(self, "_last_watchlist_realtime_refresh", 0.0) or 0.0)

        if now - last < interval:
            return

        self._last_watchlist_realtime_refresh = now

        limit = self._env_int("WATCHLIST_REALTIME_LIMIT", self.settings.realtime_symbols_limit)
        updated = self._load_market_watchlist_targets(
            limit=limit,
            trade_eligible_only=False,
            include_observe_phases=self._watchlist_realtime_observe_phases(),
        )
        if not updated:
            return

        old_symbols = {target.symbol for target in symbols}
        new_symbols = [target.symbol for target in updated if target.symbol not in old_symbols]

        changed = self._replace_scan_targets(symbols, updated, label="realtime")
        if changed and new_symbols:
            await stream.subscribe_symbols(new_symbols)

    def _refresh_macro_symbols_from_watchlist(self, symbols: list[ScanTarget]) -> None:
        if not self._watchlist_realtime_enabled():
            return

        interval = max(self._env_float("WATCHLIST_MACRO_REFRESH_SECONDS", 900.0), 60.0)
        now = time.monotonic()
        last = float(getattr(self, "_last_watchlist_macro_refresh", 0.0) or 0.0)

        if now - last < interval:
            return

        self._last_watchlist_macro_refresh = now

        limit = self._env_int("WATCHLIST_MACRO_LIMIT", self.settings.macro_symbols_limit)
        updated = self._load_market_watchlist_targets(
            limit=limit,
            trade_eligible_only=True,
        )
        if updated:
            self._replace_scan_targets(symbols, updated, label="macro")

    async def _run_market_watchlist_builder(self, reason: str = "scheduled") -> None:
        script = os.getenv("WATCHLIST_REBUILD_SCRIPT", "tools/market_watchlist_rebuild.py").strip()
        if not script:
            return

        if not os.path.exists(script):
            self.logger.warning("Market watchlist rebuild skipped: script not found: %s", script)
            return

        timeout = max(self._env_float("WATCHLIST_REBUILD_TIMEOUT_SECONDS", 900.0), 60.0)

        self.logger.info("Market watchlist rebuild started | reason=%s | script=%s", reason, script)

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                script,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.warning("Market watchlist rebuild timed out after %.0fs", timeout)
            return
        except Exception as exc:
            self.logger.warning("Market watchlist rebuild failed: %r", exc)
            return

        text = (stdout or b"").decode("utf-8", errors="replace")
        tail = "\n".join(text.strip().splitlines()[-12:])

        if proc.returncode != 0:
            self.logger.warning("Market watchlist rebuild exited rc=%s\n%s", proc.returncode, tail)
            return

        self.logger.info("Market watchlist rebuild finished | reason=%s\n%s", reason, tail)

    async def _run_market_watchlist_rebuild_loop(self) -> None:
        interval = max(self._env_float("WATCHLIST_REBUILD_INTERVAL_SECONDS", 21600.0), 600.0)

        while True:
            await asyncio.sleep(interval)
            await self._run_market_watchlist_builder("scheduled")

    async def run(self) -> None:
        rest_min_interval_seconds = max(
            self._env_float(
                "ACC_REST_MIN_INTERVAL_SECONDS",
                0.25,
            ),
            0.05,
        )
        rest_rate_limit_retries = max(
            0,
            min(
                self._env_int(
                    "ACC_REST_RATE_LIMIT_RETRIES",
                    4,
                ),
                8,
            ),
        )
        rest_rate_limit_backoff_seconds = max(
            self._env_float(
                "ACC_REST_RATE_LIMIT_BACKOFF_SECONDS",
                2.0,
            ),
            0.25,
        )

        self.logger.info(
            "Bybit REST pacing enabled | "
            "min_interval=%.2fs | "
            "rate_limit_retries=%s | "
            "backoff=%.2fs",
            rest_min_interval_seconds,
            rest_rate_limit_retries,
            rest_rate_limit_backoff_seconds,
        )

        async with BybitRestClient(
            self.settings.rest_base_url,
            timeout_seconds=self.settings.rest_timeout_seconds,
            retries=self.settings.rest_retries,
            min_interval_seconds=rest_min_interval_seconds,
            rate_limit_retries=rest_rate_limit_retries,
            rate_limit_backoff_seconds=(
                rest_rate_limit_backoff_seconds
            ),
        ) as rest:
            if self._watchlist_realtime_enabled() and self._env_bool("WATCHLIST_REBUILD_ON_STARTUP", True):
                await self._run_market_watchlist_builder("startup")

            realtime_symbols = await rest.fetch_best_symbols(
                quote_coin=self.settings.quote_coin,
                limit=self.settings.realtime_symbols_limit,
                min_notional_24h=self.settings.min_notional_24h,
                min_last_price=self.settings.min_last_price,
                market_categories=self.settings.market_categories,
                allowlist=self.settings.symbols_allowlist,
                blocklist=self.settings.symbols_blocklist,
            )

            macro_symbols = await rest.fetch_best_symbols(
                quote_coin=self.settings.quote_coin,
                limit=self.settings.macro_symbols_limit,
                min_notional_24h=self.settings.min_notional_24h,
                min_last_price=self.settings.min_last_price,
                market_categories=self.settings.market_categories,
                allowlist=self.settings.symbols_allowlist,
                blocklist=self.settings.symbols_blocklist,
            )

            realtime_symbols = self._filter_symbols(realtime_symbols)
            macro_symbols = self._filter_symbols(macro_symbols)

            realtime_symbols = self._watchlist_initial_targets(
                realtime_symbols,
                limit=self._env_int("WATCHLIST_REALTIME_LIMIT", self.settings.realtime_symbols_limit),
                label="realtime",
            )
            macro_symbols = self._watchlist_initial_targets(
                macro_symbols,
                limit=self._env_int("WATCHLIST_MACRO_LIMIT", self.settings.macro_symbols_limit),
                label="macro",
            )

            stream = MarketStream(
                url=self.settings.ws_public_url,
                book_depth=self.settings.book_depth,
                tape_window_seconds=self.settings.tape_window_seconds,
                wall_persistence_seconds=self.settings.wall_persistence_seconds,
                heartbeat_seconds=self.settings.ws_heartbeat_seconds,
            )

            await self.telegram.send_message(
                f"🚀 <b>Accumulation {self.version} started</b>\n"
                f"Realtime symbols: {len(realtime_symbols)}\n"
                f"Macro symbols: {len(macro_symbols)}\n"
                f"Mode: {'signals only' if self.settings.signals_only else 'trade ready'}\n"
                f"Signal mode: {self.settings.signal_mode}\n"
                f"market_data_testnet={self.settings.bybit_market_data_testnet}\n"
                f"order_testnet={self.settings.bybit_testnet}"
            )

            tasks = [
                asyncio.create_task(stream.run([target.symbol for target in realtime_symbols]), name="accum_ws"),
                asyncio.create_task(self._run_realtime_scan(rest, stream, realtime_symbols), name="accum_realtime"),
                asyncio.create_task(self._run_macro_scan(rest, macro_symbols), name="accum_macro"),
                asyncio.create_task(self._run_status(stream, realtime_symbols, macro_symbols), name="accum_status"),
            ]
            if self._watchlist_realtime_enabled() and self._env_bool("WATCHLIST_AUTO_REBUILD_ENABLED", True):
                tasks.append(
                    asyncio.create_task(self._run_market_watchlist_rebuild_loop(), name="accum_watchlist_rebuild")
                )
            if self.trade_executor_enabled:
                tasks.append(
                    asyncio.create_task(self._run_executor_maintenance(rest, stream), name="accum_executor_maintenance")
                )

            await asyncio.gather(*tasks)

    async def _run_status(self, stream: MarketStream, realtime_symbols: list[ScanTarget], macro_symbols: list[ScanTarget]) -> None:
        while True:
            realtime_count = len(realtime_symbols)
            macro_count = len(macro_symbols)

            await self.dashboard.post_heartbeat(
                "scanner",
                meta={
                    "runner": "orderflow_accum",
                    "loop": "status",
                    "ws_status": stream.status,
                    "realtime_symbols": realtime_count,
                    "macro_symbols": macro_count,
                    "watchlist_realtime_enabled": self._watchlist_realtime_enabled(),
                    "macro_signals": self._counts["macro"],
                    "orderflow_signals": self._counts["orderflow"],
                },
            )

            self.ui.update_session(
                ws_status=stream.status,
                macro=self._counts["macro"],
                orderflow=self._counts["orderflow"],
            )
            self.ui.print_session(realtime_count, macro_count)
            await self._post_executor_heartbeat(loop="status")

            await asyncio.sleep(30)

    async def _post_executor_heartbeat(self, *, loop: str, refreshed: int | None = None) -> None:
        if not self.trade_executor_enabled:
            return
        dashboard = getattr(self, "dashboard", None)
        post_heartbeat = getattr(dashboard, "post_heartbeat", None)
        if post_heartbeat is None:
            return
        meta = {
            "runner": "orderflow_accum",
            "loop": loop,
            "mode": self.trade_executor_mode,
        }
        if refreshed is not None:
            meta["refreshed_open_positions"] = refreshed
        try:
            await post_heartbeat("executor", status="online", meta=meta)
        except Exception:
            self.logger.debug("Executor heartbeat post failed", exc_info=True)

    async def _run_executor_maintenance(self, rest: BybitRestClient, stream: MarketStream) -> None:
        refresh_seconds = max(5, int(os.getenv("EXECUTOR_OPEN_POSITION_REFRESH_SECONDS", "30") or "30"))
        while True:
            refreshed = 0
            try:
                refreshed = await self.refresh_open_executor_positions(
                    rest=rest,
                    stream=stream,
                )
            except Exception:
                self.logger.exception(
                    "Open executor position refresh failed"
                )

            try:
                await self.refresh_deferred_entry_candidates(
                    rest=rest,
                    stream=stream,
                )
            except Exception:
                self.logger.exception(
                    "Deferred entry lifecycle refresh failed"
                )

            await self._post_executor_heartbeat(
                loop="executor_maintenance",
                refreshed=refreshed,
            )
            await asyncio.sleep(refresh_seconds)

    def _closed_candle_frame(self, df):
        """Drop the currently forming candle and keep closed history only."""
        if df is None or getattr(df, "empty", True) or len(df) < 2:
            return None

        closed = df.iloc[:-1].copy()
        if getattr(closed, "empty", True):
            return None

        return closed

    def _closed_h1_frame_once(self, *, symbol: str, market: str, interval: str, df):
        """Return a newly closed H1 frame once; skip the forming H1 candle."""
        normalized_interval = str(interval or "").strip().lower()

        if normalized_interval not in {"60", "1h", "h1"}:
            return df

        if not self._env_bool("H1_CLOSED_CANDLE_ONLY", True):
            return df

        closed = self._closed_candle_frame(df)
        if closed is None:
            return None

        bar_start = str(closed.iloc[-1].get("start", "") or "")
        if not bar_start:
            return None

        key = (
            str(symbol or "").upper(),
            str(market or "").lower(),
        )

        if self._processed_closed_h1_bars.get(key) == bar_start:
            return None

        self._processed_closed_h1_bars[key] = bar_start
        return closed

    async def _h4_long_entry_gate_context(
        self,
        rest: BybitRestClient,
        signal,
    ) -> dict[str, object]:
        """Return a conservative H4 context for future H1 Long entries.

        Scanner/search formulas are untouched. This gate only decides whether an
        executor entry is allowed after a H1 signal was already produced.
        """
        enabled = self._env_bool("EXECUTOR_H4_GATE_ENABLED", True)

        context: dict[str, object] = {
            "h4_entry_gate_enabled": enabled,
            "h4_entry_gate_allowed": True,
            "h4_entry_gate_reason": "h4_gate_not_checked",
        }

        if not enabled:
            context["h4_entry_gate_reason"] = "h4_gate_disabled"
            return context

        side = str(getattr(signal, "side", "") or "")
        timeframe = str(
            getattr(signal, "meta", {}).get("tf")
            if isinstance(getattr(signal, "meta", {}), dict)
            else ""
        ).strip().lower()

        if side != "Buy":
            context["h4_entry_gate_reason"] = "h4_gate_not_buy_signal"
            return context

        if timeframe not in {"60", "1h", "h1"}:
            if self._env_bool("EXECUTOR_H1_ONLY_ENTRY_ENABLED", True):
                context.update(
                    {
                        "h4_entry_gate_allowed": False,
                        "h4_entry_gate_reason": "entry_blocked_entry_timeframe_not_h1",
                        "h4_entry_gate_timeframe": timeframe or None,
                    }
                )
            else:
                context["h4_entry_gate_reason"] = "h4_gate_not_h1_signal"
            return context

        market = str(
            getattr(signal, "meta", {}).get("market", "linear")
            if isinstance(getattr(signal, "meta", {}), dict)
            else "linear"
        ).lower()

        try:
            raw_h4 = await rest.fetch_klines(
                str(signal.symbol),
                interval="240",
                limit=80,
                category=market,
            )
        except Exception as exc:
            context.update(
                {
                    "h4_entry_gate_allowed": False,
                    "h4_entry_gate_reason": "entry_blocked_h4_data_unavailable",
                    "h4_entry_gate_error": str(exc),
                }
            )
            return context

        closed_h4 = self._closed_candle_frame(raw_h4)
        if closed_h4 is None or len(closed_h4) < 52:
            context.update(
                {
                    "h4_entry_gate_allowed": False,
                    "h4_entry_gate_reason": "entry_blocked_h4_data_unavailable",
                    "h4_entry_gate_error": "not_enough_closed_h4_bars",
                }
            )
            return context

        frame = add_indicators(closed_h4)
        last = frame.iloc[-1]

        close = self._optional_float(last.get("close"))
        ema20 = self._optional_float(last.get("ema_20"))
        ema50 = self._optional_float(last.get("ema_50"))
        return_3 = self._optional_float(last.get("return_3"))

        if None in {close, ema20, ema50, return_3}:
            context.update(
                {
                    "h4_entry_gate_allowed": False,
                    "h4_entry_gate_reason": "entry_blocked_h4_data_unavailable",
                    "h4_entry_gate_error": "missing_h4_indicators",
                }
            )
            return context

        max_return_3 = self._env_float(
            "EXECUTOR_H4_GATE_MAX_RETURN_3",
            -0.01,
        )

        confirmed_bearish = bool(
            close < ema20
            and ema20 < ema50
            and return_3 <= max_return_3
        )

        context.update(
            {
                "h4_entry_gate_close": close,
                "h4_entry_gate_ema20": ema20,
                "h4_entry_gate_ema50": ema50,
                "h4_entry_gate_return_3": return_3,
                "h4_entry_gate_max_return_3": max_return_3,
                "h4_entry_gate_confirmed_bearish": confirmed_bearish,
            }
        )

        if confirmed_bearish:
            context.update(
                {
                    "h4_entry_gate_allowed": False,
                    "h4_entry_gate_reason": "entry_blocked_h4_bearish_structure",
                }
            )
        else:
            context["h4_entry_gate_reason"] = "h4_not_confirmed_bearish"

        return context

    async def _run_realtime_scan(
        self,
        rest: BybitRestClient,
        stream: MarketStream,
        symbols: list[ScanTarget],
    ) -> None:
        self.logger.info("Realtime accumulation loop started for %s symbols", len(symbols))

        preimpulse_intervals = {value.upper() for value in self.settings.preimpulse_intervals}
        realtime_intervals = {value.upper() for value in self.settings.realtime_intervals}

        while True:
            await self._refresh_realtime_symbols_from_watchlist(stream, symbols)

            await self.dashboard.post_heartbeat(
                "scanner",
                meta={
                    "runner": "orderflow_accum",
                    "loop": "realtime",
                    "symbols": len(symbols),
                    "watchlist_realtime_enabled": self._watchlist_realtime_enabled(),
                },
            )

            btc_frames = await self._fetch_btc_regime_frames(rest)

            regime = self.regime_analyzer.analyze_btc(btc_frames)

            for target in symbols:
                try:
                    symbol = target.symbol

                    for interval in self.settings.realtime_intervals:
                        df = await rest.fetch_klines(
                            symbol,
                            interval=interval,
                            limit=180,
                            category=target.market,
                        )

                        df = self._closed_h1_frame_once(
                            symbol=symbol,
                            market=target.market,
                            interval=interval,
                            df=df,
                        )

                        if df is None:
                            continue

                        state = stream.get_state(symbol)

                        long_signals = self.realtime_engine.analyze(symbol, df, state)

                        for signal in long_signals:
                            signal.score = round(signal.score + float(regime.long_penalty or 0.0), 2)
                            self._apply_market_regime_meta(signal, regime)

                        short_signals = []

                        if self.settings.enable_short_engine and target.market == "linear":
                            short_signals = self.short_engine.analyze(symbol, df, state, regime)

                            for signal in short_signals:
                                self._apply_market_regime_meta(signal, regime)

                        signals = long_signals + short_signals

                        if not signals:
                            reason, score, metrics = self.realtime_engine.diagnose(symbol, df, state)
                            metrics = dict(metrics or {})
                            metrics["tf"] = interval
                            self.rejection_logger.append("orderflow", symbol, reason, score, metrics)

                        for signal in signals:
                            interval_u = str(interval).upper()
                            is_preimpulse = signal.kind in self._preimpulse_kinds

                            if is_preimpulse and interval_u not in preimpulse_intervals:
                                continue

                            if not is_preimpulse and interval_u not in realtime_intervals:
                                continue

                            signal.meta["tf"] = interval
                            signal.meta["market"] = target.market

                            await self._emit_signal(rest, signal, state=state)

                except Exception as exc:
                    self.logger.warning("Realtime scan failed for %s: %r", symbol, exc)

                await asyncio.sleep(0.05)

            await asyncio.sleep(max(self.settings.realtime_scan_every_seconds, 1))


    async def _fetch_btc_regime_frames(self, rest: BybitRestClient) -> dict[str, object]:
        btc_frames: dict[str, object] = {}
        try:
            for tf in self.settings.btc_regime_intervals:
                btc_df = await rest.fetch_klines(
                    "BTCUSDT",
                    interval=tf,
                    limit=120,
                    category="linear",
                )
                btc_frames[tf] = add_indicators(btc_df) if btc_df is not None and not btc_df.empty else btc_df
        except Exception:
            return {}
        return btc_frames

    @staticmethod
    def _apply_market_regime_meta(signal, regime) -> None:
        btc_regime = str(getattr(regime, "btc_regime", "") or "BTC_NEUTRAL")
        market_regime = str(getattr(regime, "market_regime", "") or btc_regime)
        signal.meta["btc_regime"] = btc_regime
        signal.meta["market_regime"] = market_regime

    async def _run_macro_scan(self, rest: BybitRestClient, symbols: list[ScanTarget]) -> None:
        self.logger.info("Macro base scan loop started for %s symbols", len(symbols))

        intervals = {"60": 60, "240": 50, "D": 45}

        while True:
            self._refresh_macro_symbols_from_watchlist(symbols)

            await self.dashboard.post_heartbeat(
                "scanner",
                meta={
                    "runner": "orderflow_accum",
                    "loop": "macro",
                    "symbols": len(symbols),
                    "watchlist_realtime_enabled": self._watchlist_realtime_enabled(),
                },
            )

            for target in symbols:
                try:
                    symbol = target.symbol
                    frames = {}

                    for interval, limit in intervals.items():
                        raw_frame = await rest.fetch_klines(
                            symbol,
                            interval=interval,
                            limit=limit,
                            category=target.market,
                        )
                        frames[interval] = self._closed_candle_frame(raw_frame)
                        await asyncio.sleep(0.04)

                    if any(
                        frame is None or getattr(frame, "empty", True)
                        for frame in frames.values()
                    ):
                        continue

                    signal = self.macro_engine.analyze(symbol, frames)

                    if signal:
                        signal.meta["market"] = target.market
                        await self._emit_signal(rest, signal)
                    else:
                        reason, score, metrics = self.macro_engine.diagnose(symbol, frames)
                        self.rejection_logger.append("macro", symbol, reason, score, metrics)

                except Exception as exc:
                    self.logger.warning("Macro scan failed for %s: %r", symbol, exc)

                await asyncio.sleep(0.08)

            await asyncio.sleep(max(self.settings.macro_every_seconds, 120))

    async def _build_chart_for_signal(self, rest: BybitRestClient, signal) -> str | None:
        if not self.settings.telegram_send_charts:
            return None

        try:
            if signal.source == "macro":
                interval = str(signal.meta.get("tf") or "240")
                bars = self.settings.chart_bars_macro
            else:
                interval = str(signal.meta.get("tf") or "1")
                bars = self.settings.chart_bars_realtime

            market = str(signal.meta.get("market", "linear"))

            df = await rest.fetch_klines(
                signal.symbol,
                interval=interval,
                limit=bars,
                category=market,
            )

            if df.empty:
                return None

            support = signal.meta.get("support")
            resistance = signal.meta.get("resistance") or signal.meta.get("corridor_high")

            return render_signal_chart(
                df=df,
                symbol=signal.symbol,
                kind=signal.kind,
                support=float(support) if support is not None else None,
                resistance=float(resistance) if resistance is not None else None,
                entry=signal.entry,
                stop_loss=signal.stop_loss,
                take_profit_1=signal.take_profit_1,
                take_profit_2=signal.take_profit_2,
                output_dir="accum_charts",
            )

        except Exception as exc:
            self.logger.warning("Chart build failed for %s: %r", signal.symbol, exc)
            return None

    def _cooldown_seconds(self, signal) -> int:
        if signal.source == "macro":
            return self.settings.macro_symbol_cooldown_minutes * 60

        return self.settings.signal_cooldown_seconds

    def _maybe_promote_confirmed(self, signal, upsert, market: str) -> tuple[bool, str | None, list[str]]:
        setup = {
            "side": signal.side,
            "market": market,
            "status": upsert.to_status,
            "score_first": signal.score,
            "score_last": signal.score,
            "repeat_count": upsert.repeat_count,
            "timeframe": str(signal.meta.get("tf", "1")),
            "reasons": list(signal.reasons or []),
            "btc_regime": signal.meta.get("btc_regime"),
        }

        decision = self.promoter.should_promote(
            setup,
            {"reasons": list(signal.reasons or [])},
            {"btc_regime": signal.meta.get("btc_regime")},
        )

        if not decision.should_promote or not decision.target_status:
            return False, None, decision.reasons

        signal_key = f"{signal.symbol}|{market}|{setup['timeframe']}|{signal.kind}|{signal.side}"

        changed = self.signal_store.promote_signal(
            signal_key=signal_key,
            to_status=decision.target_status,
            score_last=float(signal.score),
        )

        if changed:
            signal.meta["promotion_status"] = decision.target_status
            signal.meta["promotion_reasons"] = decision.reasons

        return changed, decision.target_status, decision.reasons

    def _signal_key(self, signal, market: str) -> str:
        timeframe = str(signal.meta.get("tf") or "1")
        return f"{signal.symbol}|{market}|{timeframe}|{signal.kind}|{signal.side}"

    def _record_signal_lifecycle(self, signal, signal_key: str, upsert, confirmed_status: str | None) -> None:
        event_type = "SIGNAL_CREATED" if upsert.is_new else "SIGNAL_UPDATED"
        self.trade_learning.record_signal(
            signal=signal,
            signal_key=signal_key,
            event_type=event_type,
            status=upsert.to_status,
            features={
                "repeat_count": upsert.repeat_count,
                "status_changed": upsert.status_changed,
                "score_jump": upsert.score_jump,
                "from_status": upsert.from_status,
                "to_status": upsert.to_status,
            },
        )

        if confirmed_status in {"CONFIRMED_LONG", "CONFIRMED_SHORT"}:
            self.trade_learning.record_signal(
                signal=signal,
                signal_key=signal_key,
                event_type="CONFIRMED",
                status=confirmed_status,
                features={
                    "promotion_status": signal.meta.get("promotion_status"),
                    "promotion_reasons": signal.meta.get("promotion_reasons", []),
                },
            )

    def _testnet_observation_entry_context(self, signal, confirmed_status: str | None) -> dict[str, object]:
        status = str(confirmed_status or "").strip().upper()
        kind = str(getattr(signal, "kind", "") or "").strip().upper()
        side = str(getattr(signal, "side", "") or "").strip()
        score = self._optional_float(getattr(signal, "score", None)) or 0.0
        meta = getattr(signal, "meta", {}) if isinstance(getattr(signal, "meta", {}), dict) else {}
        btc_regime = str(meta.get("btc_regime") or "BTC_NEUTRAL").strip().upper()
        context: dict[str, object] = {
            "testnet_observation_entry_candidate": False,
            "testnet_observation_entry_reason": "not_testnet_observation_entry",
            "original_signal_status": status or None,
            "original_signal_kind": kind or None,
            "original_signal_score": score,
        }
        if self._normalize_trade_executor_mode(getattr(self, "trade_executor_mode", "paper")) != "testnet":
            context["testnet_observation_entry_reason"] = "not_testnet_mode"
            return context
        if status in {"CONFIRMED_LONG", "CONFIRMED_SHORT"}:
            context["testnet_observation_entry_reason"] = "confirmed_signal_uses_standard_executor_path"
            return context
        if side != "Buy":
            context["testnet_observation_entry_reason"] = "not_buy_signal"
            return context
        if kind not in self.TESTNET_OBSERVATION_ENTRY_KINDS:
            context["testnet_observation_entry_reason"] = "signal_kind_not_allowed"
            return context
        if status not in self.TESTNET_OBSERVATION_ENTRY_STATUSES:
            context["testnet_observation_entry_reason"] = "signal_status_not_allowed"
            return context
        required_score = 9.0 if kind == "ABSORPTION_ZONE" else 8.0
        if score < required_score:
            context["testnet_observation_entry_reason"] = "score_below_testnet_observation_threshold"
            return context
        if btc_regime in self.TESTNET_OBSERVATION_BLOCKED_BTC_REGIMES:
            context["testnet_observation_entry_reason"] = "btc_regime_blocks_testnet_observation_entry"
            return context
        context["testnet_observation_entry_candidate"] = True
        context["testnet_observation_entry_reason"] = "strong_non_confirmed_buy_signal"
        return context

    def _paper_observation_entry_context(
        self,
        signal,
        confirmed_status: str | None,
    ) -> dict[str, object]:
        context = self._testnet_observation_entry_context(signal, confirmed_status)

        mode = self._normalize_trade_executor_mode(
            getattr(self, "trade_executor_mode", "paper")
        )
        status = str(confirmed_status or "").strip().upper()
        kind = str(getattr(signal, "kind", "") or "").strip().upper()
        side = str(getattr(signal, "side", "") or "").strip()
        score = self._optional_float(getattr(signal, "score", None)) or 0.0
        meta = (
            getattr(signal, "meta", {})
            if isinstance(getattr(signal, "meta", {}), dict)
            else {}
        )
        btc_regime = str(meta.get("btc_regime") or "BTC_NEUTRAL").strip().upper()

        context.update(
            {
                "paper_observation_entry_candidate": False,
                "paper_observation_entry_reason": "paper_observation_disabled",
                "paper_observation_status": status or None,
                "paper_observation_kind": kind or None,
                "paper_observation_score": score,
            }
        )

        if mode != "paper":
            context["paper_observation_entry_reason"] = "not_paper_mode"
            return context

        if not self._env_bool("EXECUTOR_PAPER_OBSERVATION_ENTRY_ENABLED", False):
            return context

        if status in {"CONFIRMED_LONG", "CONFIRMED_SHORT"}:
            context["paper_observation_entry_reason"] = (
                "confirmed_signal_uses_standard_executor_path"
            )
            return context

        allowed_statuses = {
            item.strip().upper()
            for item in os.getenv(
                "EXECUTOR_PAPER_OBSERVATION_ALLOWED_STATUSES",
                "PRE_IMPULSE,BREAKOUT_PRESSURE,PENDING",
            ).split(",")
            if item.strip()
        }

        allowed_kinds = {
            item.strip().upper()
            for item in os.getenv(
                "EXECUTOR_PAPER_OBSERVATION_ALLOWED_KINDS",
                "PRE_IMPULSE_ZONE,BREAKOUT_PRESSURE,ACCUMULATION_LONG_READY",
            ).split(",")
            if item.strip()
        }

        min_score = max(
            self._env_float("EXECUTOR_PAPER_OBSERVATION_MIN_SCORE", 10.0),
            0.0,
        )

        if side != "Buy":
            context["paper_observation_entry_reason"] = "not_buy_signal"
            return context

        if kind not in allowed_kinds:
            context["paper_observation_entry_reason"] = "signal_kind_not_allowed"
            return context

        if status not in allowed_statuses:
            context["paper_observation_entry_reason"] = "signal_status_not_allowed"
            return context

        if score < min_score:
            context["paper_observation_entry_reason"] = (
                "score_below_paper_observation_threshold"
            )
            return context

        if btc_regime in self.TESTNET_OBSERVATION_BLOCKED_BTC_REGIMES:
            context["paper_observation_entry_reason"] = (
                "btc_regime_blocks_paper_observation_entry"
            )
            return context

        context["paper_observation_entry_candidate"] = True
        context["paper_observation_entry_reason"] = "strong_early_buy_signal"
        return context

    def _should_process_paper_executor_status(
        self,
        signal,
        confirmed_status: str | None,
    ) -> tuple[bool, dict[str, object]]:
        context = self._paper_observation_entry_context(signal, confirmed_status)

        if confirmed_status in {"CONFIRMED_LONG", "CONFIRMED_SHORT"}:
            return True, context

        return bool(
            context.get("paper_observation_entry_candidate")
            or context.get("testnet_observation_entry_candidate")
        ), context

    def _executor_allowed_sides(self) -> set[str]:
        raw = os.getenv("EXECUTOR_ALLOWED_SIDES", "Buy,Sell")
        allowed: set[str] = set()
        for item in raw.split(","):
            side = item.strip().lower()
            if side in {"buy", "long"}:
                allowed.add("Buy")
            elif side in {"sell", "short"}:
                allowed.add("Sell")
        return allowed or {"Buy", "Sell"}

    def _executor_side_allowed(self, side: str) -> bool:
        return str(side) in self._executor_allowed_sides()

    def _executor_buy_momentum_symbol_blocked(self, symbol: str) -> bool:
        raw = os.getenv(
            "EXECUTOR_BUY_MOMENTUM_SYMBOL_BLOCKLIST",
            os.getenv("EXECUTOR_SYMBOL_BLOCKLIST", "BTCUSDT"),
        )
        blocked = {item.strip().upper() for item in raw.split(",") if item.strip()}
        return str(symbol or "").upper() in blocked

    def _executor_buy_momentum_override_decision(self, setup, snapshot, entry_decision):
        if not self._env_bool("EXECUTOR_BUY_MOMENTUM_OVERRIDE_ENABLED", True):
            return entry_decision

        if str(getattr(setup, "side", "")) != "Buy":
            return entry_decision

        if hasattr(self, "_executor_side_allowed") and not self._executor_side_allowed("Buy"):
            return entry_decision

        if str(getattr(entry_decision, "action", "")) in {ENTER_LONG, ENTER_SHORT}:
            return entry_decision

        reason = str(getattr(entry_decision, "reason", "") or "")
        raw_reasons = os.getenv(
            "EXECUTOR_BUY_MOMENTUM_OVERRIDE_REASONS",
            "entry_blocked_absorption_weak_confirmation,entry_blocked_ask_wall",
        )
        allowed_reasons = {item.strip() for item in raw_reasons.split(",") if item.strip()}
        if reason not in allowed_reasons:
            return entry_decision

        if self._executor_buy_momentum_symbol_blocked(getattr(setup, "symbol", "")):
            return entry_decision

        buy_flow = float(getattr(snapshot, "buy_flow", 0.0) or 0.0)
        sell_flow = float(getattr(snapshot, "sell_flow", 0.0) or 0.0)
        volume_impulse = float(getattr(snapshot, "volume_impulse", 0.0) or 0.0)

        if buy_flow <= 0:
            return entry_decision

        flow_ratio = buy_flow / max(sell_flow, 1e-9)

        min_flow_ratio = self._env_float("EXECUTOR_BUY_MOMENTUM_MIN_FLOW_RATIO", 2.5)
        min_volume_impulse = self._env_float("EXECUTOR_BUY_MOMENTUM_MIN_VOLUME_IMPULSE", 2.0)

        if reason == "entry_blocked_ask_wall":
            min_flow_ratio = self._env_float("EXECUTOR_BUY_MOMENTUM_ASK_WALL_MIN_FLOW_RATIO", 4.0)
            min_volume_impulse = self._env_float("EXECUTOR_BUY_MOMENTUM_ASK_WALL_MIN_VOLUME_IMPULSE", 2.5)

            ask_wall = float(getattr(snapshot, "ask_wall_strength", 0.0) or 0.0)
            bid_wall = float(getattr(snapshot, "bid_wall_strength", 0.0) or 0.0)
            max_ask_to_bid = self._env_float("EXECUTOR_BUY_MOMENTUM_MAX_ASK_TO_BID_WALL", 3.0)

            if bid_wall > 0 and ask_wall > bid_wall * max_ask_to_bid:
                return entry_decision

        if flow_ratio < min_flow_ratio:
            return entry_decision

        if volume_impulse < min_volume_impulse:
            return entry_decision

        return TradeDecision(
            ENTER_LONG,
            "entry_allowed_buy_momentum_override",
            ENTERED,
            None,
        )

    def _paper_executor_setup(self, signal) -> TradeSetup:
        return TradeSetup(
            symbol=str(signal.symbol),
            side=str(signal.side),
            entry_hint=float(signal.entry or 0.0),
            stop_loss=float(signal.stop_loss or 0.0),
            score=float(signal.score or 0.0),
            timeframe=str(signal.meta.get("tf") or "1"),
            btc_regime=str(signal.meta.get("btc_regime") or "BTC_NEUTRAL"),
            reasons=list(signal.reasons or []),
            signal_kind=str(getattr(signal, "kind", "") or ""),
            market_regime=str(signal.meta.get("market_regime") or signal.meta.get("btc_regime") or "BTC_NEUTRAL"),
        )


    def _symbol_position_lock_enabled(self) -> bool:
        return self._env_bool("EXECUTOR_SYMBOL_POSITION_LOCK", True)

    def _executor_symbol_position_lock(self, signal_key: str, setup: TradeSetup) -> tuple[TradeDecision | None, dict[str, object]]:
        """Block duplicate active executor positions for the same symbol.

        This does not affect scanner/search logic. It only prevents the executor
        from opening another paper/testnet/live position on a symbol that already
        has an active position under another signal key/timeframe/side.
        """
        diagnostics = {
            "symbol_position_lock_enabled": self._symbol_position_lock_enabled(),
            "symbol_position_lock_blocked": False,
        }
        if not self._symbol_position_lock_enabled():
            return None, diagnostics

        symbol = str(setup.symbol or "").strip()
        if not symbol:
            return None, diagnostics

        db_path = getattr(getattr(self, "signal_store", None), "db_path", "data/signals.db")
        conn = None
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT signal_key, symbol, side, state, action, entry_price, updated_at
                FROM executor_outcomes
                WHERE symbol = ?
                  AND COALESCE(signal_key, '') <> ?
                  AND UPPER(COALESCE(state, '')) NOT IN ('EXITED', 'CLOSED')
                  AND (
                        UPPER(COALESCE(state, '')) IN ('ENTERED', 'PROTECT_BREAKEVEN', 'TRAILING_PROFIT')
                        OR UPPER(COALESCE(action, '')) = 'HOLD'
                  )
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (symbol, signal_key),
            ).fetchone()
        except Exception as exc:
            diagnostics["symbol_position_lock_error"] = str(exc)
            return None, diagnostics
        finally:
            if conn is not None:
                conn.close()

        if row is None:
            return None, diagnostics

        existing_side = str(row["side"] or "")
        reason = (
            "entry_blocked_opposite_side_already_open"
            if existing_side and existing_side != str(setup.side)
            else "entry_blocked_symbol_already_open"
        )

        diagnostics.update({
            "symbol_position_lock_blocked": True,
            "existing_signal_key": row["signal_key"],
            "existing_symbol": row["symbol"],
            "existing_side": existing_side,
            "existing_state": row["state"],
            "existing_action": row["action"],
            "existing_entry_price": row["entry_price"],
            "existing_updated_at": row["updated_at"],
            "blocked_new_side": str(setup.side),
            "blocked_reason": reason,
        })

        return TradeDecision(WATCH, reason, "TRADE_WATCH", None), diagnostics

    def _rr_guard_enabled(self) -> bool:
        return self._env_bool("EXECUTOR_RR_GUARD_ENABLED", True)

    def _rr_guard_max_risk_pct(self, setup: TradeSetup) -> float:
        tf = str(setup.timeframe or "").strip().lower()
        if tf in {"1", "3", "5", "15", "30"}:
            return max(self._env_float("EXECUTOR_RR_GUARD_MAX_RISK_PCT_FAST", 2.5), 0.0)
        if tf in {"60", "1h"}:
            return max(self._env_float("EXECUTOR_RR_GUARD_MAX_RISK_PCT_1H", 4.0), 0.0)
        if tf in {"240", "4h"}:
            return max(self._env_float("EXECUTOR_RR_GUARD_MAX_RISK_PCT_4H", 7.0), 0.0)
        return max(self._env_float("EXECUTOR_RR_GUARD_MAX_RISK_PCT_DEFAULT", 3.0), 0.0)

    def _entry_risk_reward_guard(self, signal, setup: TradeSetup, snapshot: OrderflowSnapshot) -> tuple[TradeDecision | None, dict[str, object]]:
        """Block executor entries with far SL or bad reward/risk.

        This does not change scanner/search logic. It only prevents the executor
        from opening mathematically bad trades where stop is far and target is close.
        """
        diagnostics: dict[str, object] = {
            "rr_guard_enabled": self._rr_guard_enabled(),
            "rr_guard_blocked": False,
        }

        if not self._rr_guard_enabled():
            return None, diagnostics

        if self.trade_executor is None:
            return None, diagnostics

        entry_price = self._optional_float(getattr(snapshot, "price", None))
        if entry_price is None or entry_price <= 0:
            return None, diagnostics

        try:
            initial_sl = self.trade_executor._initial_stop_loss(setup, snapshot)
        except Exception as exc:
            diagnostics["rr_guard_error"] = str(exc)
            return None, diagnostics

        tp1 = self._optional_float(getattr(signal, "take_profit_1", None))
        tp2 = self._optional_float(getattr(signal, "take_profit_2", None))

        side = str(setup.side or "")
        if side == "Buy":
            risk = entry_price - initial_sl
            reward1 = (tp1 - entry_price) if tp1 is not None else None
            reward2 = (tp2 - entry_price) if tp2 is not None else None
        elif side == "Sell":
            risk = initial_sl - entry_price
            reward1 = (entry_price - tp1) if tp1 is not None else None
            reward2 = (entry_price - tp2) if tp2 is not None else None
        else:
            return None, diagnostics

        if risk <= 0:
            return None, diagnostics

        risk_pct = (risk / entry_price) * 100.0
        reward1_pct = (reward1 / entry_price) * 100.0 if reward1 is not None else None
        reward2_pct = (reward2 / entry_price) * 100.0 if reward2 is not None else None
        rr1 = (reward1 / risk) if reward1 is not None and reward1 > 0 else None
        rr2 = (reward2 / risk) if reward2 is not None and reward2 > 0 else None

        max_risk_pct = self._rr_guard_max_risk_pct(setup)
        min_rr_tp1 = max(self._env_float("EXECUTOR_RR_GUARD_MIN_RR_TP1", 0.8), 0.0)
        min_rr_tp2 = max(self._env_float("EXECUTOR_RR_GUARD_MIN_RR_TP2", 1.3), 0.0)

        diagnostics.update({
            "rr_guard_side": side,
            "rr_guard_timeframe": str(setup.timeframe),
            "rr_guard_entry_price": float(entry_price),
            "rr_guard_initial_sl": float(initial_sl),
            "rr_guard_tp1": tp1,
            "rr_guard_tp2": tp2,
            "rr_guard_risk_pct": float(risk_pct),
            "rr_guard_reward1_pct": reward1_pct,
            "rr_guard_reward2_pct": reward2_pct,
            "rr_guard_rr1": rr1,
            "rr_guard_rr2": rr2,
            "rr_guard_max_risk_pct": float(max_risk_pct),
            "rr_guard_min_rr_tp1": float(min_rr_tp1),
            "rr_guard_min_rr_tp2": float(min_rr_tp2),
        })

        if max_risk_pct > 0 and risk_pct > max_risk_pct:
            diagnostics["rr_guard_blocked"] = True
            diagnostics["rr_guard_block_reason"] = "entry_blocked_stop_loss_too_far"
            return TradeDecision(WATCH, "entry_blocked_stop_loss_too_far", "TRADE_WATCH", None), diagnostics

        has_rr_target = rr1 is not None or rr2 is not None
        rr_ok = False
        if rr2 is not None and rr2 >= min_rr_tp2:
            rr_ok = True
        if rr1 is not None and rr1 >= min_rr_tp1:
            rr_ok = True

        if has_rr_target and not rr_ok:
            diagnostics["rr_guard_blocked"] = True
            diagnostics["rr_guard_block_reason"] = "entry_blocked_bad_rr"
            return TradeDecision(WATCH, "entry_blocked_bad_rr", "TRADE_WATCH", None), diagnostics

        return None, diagnostics

    def _executor_target_quality_gate(
        self,
        signal,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
        confirmed_status: str | None,
    ) -> tuple[TradeDecision | None, dict[str, object]]:
        """Block executor entries with weak targets without changing scanner logic.

        Signals are still discovered, stored and reported. This gate affects only
        paper/testnet/live executor admission after an entry decision exists.
        """
        enabled = self._env_bool("EXECUTOR_TARGET_QUALITY_GATE_ENABLED", True)

        kind = str(getattr(signal, "kind", "") or "").strip().upper()
        status = str(confirmed_status or "").strip().upper()
        side = str(getattr(setup, "side", "") or "").strip()
        timeframe = str(getattr(setup, "timeframe", "") or "").strip().lower()

        diagnostics: dict[str, object] = {
            "target_quality_gate_enabled": enabled,
            "target_quality_gate_allowed": True,
            "target_quality_kind": kind or None,
            "target_quality_status": status or None,
            "target_quality_side": side or None,
            "target_quality_timeframe": timeframe or None,
        }

        if not enabled:
            diagnostics["target_quality_reason"] = "target_quality_gate_disabled"
            return None, diagnostics

        # The current executor policy is Buy-only. Preserve Sell behavior here so
        # this gate cannot alter scanner/short-engine discovery.
        if side != "Buy":
            diagnostics["target_quality_reason"] = "target_quality_not_buy_side"
            return None, diagnostics

        allowed_kinds = {
            item.strip().upper()
            for item in os.getenv(
                "EXECUTOR_TARGET_QUALITY_ALLOWED_KINDS",
                "PRE_IMPULSE_ZONE,BREAKOUT_PRESSURE,ACCUMULATION_LONG_READY",
            ).split(",")
            if item.strip()
        }

        allowed_statuses = {
            item.strip().upper()
            for item in os.getenv(
                "EXECUTOR_TARGET_QUALITY_ALLOWED_STATUSES",
                "PRE_IMPULSE,BREAKOUT_PRESSURE,PENDING,CONFIRMED_LONG",
            ).split(",")
            if item.strip()
        }

        # A promoted CONFIRMED_LONG can retain its original scanner kind
        # (for example ABSORPTION_ZONE), so confirmation is accepted explicitly.
        kind_allowed = kind in allowed_kinds or status == "CONFIRMED_LONG"
        status_allowed = status in allowed_statuses

        diagnostics.update(
            {
                "target_quality_allowed_kinds": sorted(allowed_kinds),
                "target_quality_allowed_statuses": sorted(allowed_statuses),
                "target_quality_kind_allowed": kind_allowed,
                "target_quality_status_allowed": status_allowed,
            }
        )

        if not kind_allowed:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_kind_not_allowed",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_kind_not_allowed",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        if not status_allowed:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_status_not_allowed",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_status_not_allowed",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        entry_price = self._optional_float(getattr(snapshot, "price", None))
        tp1 = self._optional_float(getattr(signal, "take_profit_1", None))

        try:
            initial_sl = self.trade_executor._initial_stop_loss(setup, snapshot)
        except Exception as exc:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_invalid_stop",
                    "target_quality_error": str(exc),
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_invalid_stop",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        if entry_price is None or entry_price <= 0 or initial_sl is None:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_invalid_price",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_invalid_price",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        risk = entry_price - float(initial_sl)

        if risk <= 0:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_invalid_stop",
                    "target_quality_entry_price": entry_price,
                    "target_quality_initial_sl": float(initial_sl),
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_invalid_stop",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        risk_pct = risk / entry_price * 100.0
        max_risk_pct = max(
            self._env_float(
                "EXECUTOR_TARGET_QUALITY_MAX_RISK_PCT_1H",
                self._rr_guard_max_risk_pct(setup),
            ),
            0.0,
        )

        diagnostics.update(
            {
                "target_quality_entry_price": entry_price,
                "target_quality_initial_sl": float(initial_sl),
                "target_quality_risk_pct": risk_pct,
                "target_quality_max_risk_pct": max_risk_pct,
                "target_quality_tp1": tp1,
            }
        )

        if max_risk_pct > 0 and risk_pct > max_risk_pct:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_stop_too_far",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_stop_too_far",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        if tp1 is None or tp1 <= entry_price:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_tp1_invalid",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_tp1_invalid",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        tp1_distance_pct = (tp1 - entry_price) / entry_price * 100.0
        rr_tp1 = (tp1 - entry_price) / risk

        min_tp1_distance_pct = max(
            self._env_float(
                "EXECUTOR_TARGET_QUALITY_MIN_TP1_DISTANCE_PCT",
                0.20,
            ),
            0.0,
        )

        min_rr_tp1 = max(
            self._env_float(
                "EXECUTOR_TARGET_QUALITY_MIN_RR_TP1",
                0.80,
            ),
            0.0,
        )

        diagnostics.update(
            {
                "target_quality_tp1_distance_pct": tp1_distance_pct,
                "target_quality_min_tp1_distance_pct": min_tp1_distance_pct,
                "target_quality_rr_tp1": rr_tp1,
                "target_quality_min_rr_tp1": min_rr_tp1,
            }
        )

        if tp1_distance_pct < min_tp1_distance_pct:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_micro_tp1",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_micro_tp1",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        if rr_tp1 < min_rr_tp1:
            diagnostics.update(
                {
                    "target_quality_gate_allowed": False,
                    "target_quality_reason": "entry_blocked_target_quality_rr_tp1",
                }
            )
            return (
                TradeDecision(
                    WATCH,
                    "entry_blocked_target_quality_rr_tp1",
                    "TRADE_WATCH",
                    None,
                ),
                diagnostics,
            )

        diagnostics["target_quality_reason"] = "target_quality_passed"
        return None, diagnostics

    def _stop_reclaim_reentry_enabled(self) -> bool:
        return self._env_bool("EXECUTOR_STOP_RECLAIM_REENTRY_ENABLED", True)

    def _latest_stop_loss_trade_for_reentry(self, setup: TradeSetup) -> dict[str, object] | None:
        db_path = getattr(getattr(self, "signal_store", None), "db_path", "data/signals.db")
        conn = None
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT *
                FROM executor_trades
                WHERE symbol = ?
                  AND side = ?
                  AND exit_reason = 'exit_stop_loss_hit'
                ORDER BY exit_time DESC
                LIMIT 1
                """,
                (setup.symbol, setup.side),
            ).fetchone()
            return dict(row) if row is not None else None
        except Exception as exc:
            self.logger.debug("Stop reclaim lookup failed for %s: %s", setup.symbol, exc)
            return None
        finally:
            if conn is not None:
                conn.close()

    def _recent_stop_loss_count_for_reentry(self, setup: TradeSetup, lookback_hours: float) -> int:
        db_path = getattr(getattr(self, "signal_store", None), "db_path", "data/signals.db")
        cutoff = datetime.fromtimestamp(time.time() - lookback_hours * 3600.0, UTC).isoformat()
        conn = None
        try:
            conn = sqlite3.connect(str(db_path))
            row = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM executor_trades
                WHERE symbol = ?
                  AND side = ?
                  AND exit_reason = 'exit_stop_loss_hit'
                  AND exit_time >= ?
                """,
                (setup.symbol, setup.side, cutoff),
            ).fetchone()
            return int(row[0] if row is not None else 0)
        except Exception:
            return 0
        finally:
            if conn is not None:
                conn.close()

    def _evaluate_stop_reclaim_reentry(
        self,
        signal_key: str,
        signal,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
    ) -> tuple[TradeDecision | None, dict[str, object]]:
        """Allow one strict re-entry after a stop if price reclaims the stop level.

        This is executor-only. It does not change scanner/search logic.
        """
        diagnostics: dict[str, object] = {
            "stop_reclaim_reentry_enabled": self._stop_reclaim_reentry_enabled(),
            "stop_reclaim_reentry_allowed": False,
        }
        if not self._stop_reclaim_reentry_enabled():
            return None, diagnostics

        lookback_hours = max(self._env_float("EXECUTOR_STOP_RECLAIM_LOOKBACK_HOURS", 24.0), 0.0)
        cooldown_minutes = max(self._env_float("EXECUTOR_STOP_RECLAIM_COOLDOWN_MINUTES", 5.0), 0.0)
        max_recent_stops = max(self._env_int("EXECUTOR_STOP_RECLAIM_MAX_RECENT_STOPS", 1), 0)
        reclaim_buffer_bps = max(self._env_float("EXECUTOR_STOP_RECLAIM_BUFFER_BPS", 5.0), 0.0)
        min_volume_impulse = max(self._env_float("EXECUTOR_STOP_RECLAIM_MIN_VOLUME_IMPULSE", 0.75), 0.0)
        flow_ratio = max(self._env_float("EXECUTOR_STOP_RECLAIM_FLOW_RATIO", 1.05), 1.0)

        row = self._latest_stop_loss_trade_for_reentry(setup)
        if row is None:
            diagnostics["stop_reclaim_reason"] = "no_recent_stop_loss"
            return None, diagnostics

        exit_time = row.get("exit_time")
        exit_dt = self._parse_executor_time(exit_time)
        if exit_dt is None:
            diagnostics["stop_reclaim_reason"] = "missing_stop_exit_time"
            return None, diagnostics

        age_minutes = (datetime.now(UTC) - exit_dt).total_seconds() / 60.0
        if age_minutes < cooldown_minutes:
            diagnostics["stop_reclaim_reason"] = "cooldown_after_stop"
            diagnostics["stop_reclaim_age_minutes"] = age_minutes
            return None, diagnostics
        if age_minutes > lookback_hours * 60.0:
            diagnostics["stop_reclaim_reason"] = "stop_too_old"
            diagnostics["stop_reclaim_age_minutes"] = age_minutes
            return None, diagnostics

        recent_stops = self._recent_stop_loss_count_for_reentry(setup, lookback_hours)
        if max_recent_stops > 0 and recent_stops > max_recent_stops:
            diagnostics["stop_reclaim_reason"] = "too_many_recent_stops"
            diagnostics["stop_reclaim_recent_stops"] = recent_stops
            return None, diagnostics

        exit_price = self._optional_float(row.get("exit_price"))
        current_price = self._optional_float(getattr(snapshot, "price", None))
        if exit_price is None or exit_price <= 0 or current_price is None or current_price <= 0:
            diagnostics["stop_reclaim_reason"] = "missing_stop_or_current_price"
            return None, diagnostics

        buffer = reclaim_buffer_bps / 10000.0
        side = str(setup.side)
        if side == "Buy":
            reclaimed = current_price >= exit_price * (1.0 + buffer)
            flow_ok = snapshot.buy_flow >= snapshot.sell_flow * flow_ratio
            wall_ok = snapshot.ask_wall_strength <= float(getattr(self.trade_executor, "ask_wall_entry_limit", 0.65))
            structure_ok = snapshot.support is not None and float(snapshot.support) < current_price
            action = ENTER_LONG
        elif side == "Sell":
            reclaimed = current_price <= exit_price * (1.0 - buffer)
            flow_ok = snapshot.sell_flow >= snapshot.buy_flow * flow_ratio
            wall_ok = snapshot.bid_wall_strength <= float(getattr(self.trade_executor, "bid_wall_entry_limit", 0.65))
            structure_ok = snapshot.resistance is not None and float(snapshot.resistance) > current_price
            action = ENTER_SHORT
        else:
            return None, diagnostics

        volume_ok = snapshot.volume_impulse >= min_volume_impulse
        allowed = bool(reclaimed and flow_ok and wall_ok and volume_ok and structure_ok)

        diagnostics.update({
            "stop_reclaim_signal_key": signal_key,
            "stop_reclaim_previous_signal_key": row.get("signal_key"),
            "stop_reclaim_exit_price": exit_price,
            "stop_reclaim_current_price": current_price,
            "stop_reclaim_age_minutes": age_minutes,
            "stop_reclaim_recent_stops": recent_stops,
            "stop_reclaim_reclaimed": bool(reclaimed),
            "stop_reclaim_flow_ok": bool(flow_ok),
            "stop_reclaim_wall_ok": bool(wall_ok),
            "stop_reclaim_volume_ok": bool(volume_ok),
            "stop_reclaim_structure_ok": bool(structure_ok),
            "stop_reclaim_reentry_allowed": allowed,
            "stop_reclaim_reason": "entry_allowed_stop_reclaim_reentry" if allowed else "stop_reclaim_conditions_not_met",
        })

        if not allowed:
            return None, diagnostics

        return TradeDecision(action, "entry_allowed_stop_reclaim_reentry", ENTERED, None), diagnostics

    def _executor_symbol_blocked(self, symbol: str) -> bool:
        raw = os.getenv("EXECUTOR_SYMBOL_BLOCKLIST", "")
        blocked = {item.strip().upper() for item in raw.split(",") if item.strip()}
        return str(symbol or "").upper() in blocked

    def _entry_stop_loss_guard(self, setup: TradeSetup, snapshot: OrderflowSnapshot) -> tuple[TradeDecision | None, dict[str, object]]:
        """Block executor entries with invalid initial SL before opening/storing a trade.

        This does not affect scanner/search logic. It only prevents impossible executor risk:
        - Buy must have initial SL below entry
        - Sell must have initial SL above entry
        """
        diagnostics: dict[str, object] = {
            "entry_stop_loss_guard_enabled": True,
            "entry_stop_loss_guard_blocked": False,
        }
        if self.trade_executor is None:
            return None, diagnostics

        entry_price = self._optional_float(getattr(snapshot, "price", None))
        if entry_price is None or entry_price <= 0:
            diagnostics.update(
                {
                    "entry_stop_loss_guard_blocked": True,
                    "invalid_entry_price": entry_price,
                }
            )
            return TradeDecision(WATCH, "entry_blocked_invalid_entry_price", "TRADE_WATCH", None), diagnostics

        try:
            initial_sl = self.trade_executor._initial_stop_loss(setup, snapshot)
        except Exception as exc:
            diagnostics.update(
                {
                    "entry_stop_loss_guard_blocked": True,
                    "invalid_stop_loss_error": str(exc),
                }
            )
            return TradeDecision(WATCH, "entry_blocked_invalid_stop_loss", "TRADE_WATCH", None), diagnostics

        diagnostics.update(
            {
                "entry_stop_loss_guard_entry_price": float(entry_price),
                "entry_stop_loss_guard_initial_sl": float(initial_sl),
                "entry_stop_loss_guard_side": str(setup.side),
            }
        )

        if self._executor_initial_sl_invalid(side=setup.side, entry_price=entry_price, initial_sl=initial_sl):
            diagnostics["entry_stop_loss_guard_blocked"] = True
            return TradeDecision(WATCH, "entry_blocked_invalid_stop_loss", "TRADE_WATCH", None), diagnostics

        return None, diagnostics

    _VOLUME_IMPULSE_META_FIELDS = (
        "volume_impulse",
        "volume_spike",
        "v_spike",
        "vspike",
        "volume_ratio",
        "volume_expansion",
    )
    _VOLUME_BASELINE_META_FIELDS = (
        "volume_baseline",
        "avg_volume",
        "average_volume",
        "baseline_volume",
        "avg_tape_notional",
        "tape_baseline",
    )
    _VOLUME_CURRENT_META_FIELDS = ("volume_current", "current_volume", "tape_total", "turnover_build")

    def _record_volume_impulse_diagnostics(self, signal, diagnostics: dict[str, object]) -> None:
        meta = getattr(signal, "meta", None)
        if isinstance(meta, dict):
            meta["_paper_volume_impulse_diagnostics"] = diagnostics

    def _volume_impulse_from_meta(self, meta: dict[str, object], source_prefix: str) -> dict[str, object] | None:
        for field in self._VOLUME_IMPULSE_META_FIELDS:
            value = self._optional_float(meta.get(field))
            if value is not None and value > 0:
                return {
                    "volume_impulse": value,
                    "volume_impulse_source": f"{source_prefix}.{field}",
                    "volume_impulse_missing": False,
                    "volume_impulse_raw": meta.get(field),
                    "volume_baseline": None,
                    "volume_current": None,
                }
        return None

    def _volume_impulse_from_baseline_meta(self, meta: dict[str, object]) -> dict[str, object] | None:
        current = next(
            (value for field in self._VOLUME_CURRENT_META_FIELDS if (value := self._optional_float(meta.get(field))) is not None),
            None,
        )
        baseline = next(
            (value for field in self._VOLUME_BASELINE_META_FIELDS if (value := self._optional_float(meta.get(field))) is not None),
            None,
        )
        if current is not None and current > 0 and baseline is not None and baseline > 0:
            return {
                "volume_impulse": current / baseline,
                "volume_impulse_source": "meta.volume_current_baseline",
                "volume_impulse_missing": False,
                "volume_impulse_raw": current / baseline,
                "volume_baseline": baseline,
                "volume_current": current,
            }
        return None

    def _volume_impulse_from_state(self, state, buy_flow: float, sell_flow: float) -> dict[str, object] | None:
        current = buy_flow + sell_flow
        baseline = None
        if state is not None:
            for field in self._VOLUME_BASELINE_META_FIELDS:
                baseline = self._optional_float(getattr(state, field, None))
                if baseline is not None and baseline > 0:
                    break
            else:
                baseline = None

            trades = sorted(
                list(getattr(state, "trades", []) or []),
                key=lambda item: float(getattr(item, "ts", 0.0) or 0.0),
            )
            if baseline is None and len(trades) >= 4:
                first_ts = float(getattr(trades[0], "ts", 0.0) or 0.0)
                last_ts = float(getattr(trades[-1], "ts", 0.0) or 0.0)
                if last_ts > first_ts:
                    midpoint = first_ts + (last_ts - first_ts) / 2.0
                    older = [trade for trade in trades if float(getattr(trade, "ts", 0.0) or 0.0) < midpoint]
                    recent = [trade for trade in trades if float(getattr(trade, "ts", 0.0) or 0.0) >= midpoint]
                    older_notional = sum(float(getattr(trade, "notional", 0.0) or 0.0) for trade in older)
                    recent_notional = sum(float(getattr(trade, "notional", 0.0) or 0.0) for trade in recent)
                    if older_notional > 0 and recent_notional > 0:
                        current = recent_notional
                        baseline = older_notional

        if current > 0 and baseline is not None and baseline > 0:
            return {
                "volume_impulse": current / baseline,
                "volume_impulse_source": "orderflow_tape_baseline",
                "volume_impulse_missing": False,
                "volume_impulse_raw": current / baseline,
                "volume_baseline": baseline,
                "volume_current": current,
            }
        return None

    def _volume_impulse_from_reasons(self, signal) -> dict[str, object] | None:
        reasons = [str(reason).lower() for reason in (getattr(signal, "reasons", []) or [])]
        volume_reason_tokens = ("volume", "turnover", "tape", "flow", "impulse", "breakout")
        if not any(any(token in reason for token in volume_reason_tokens) for reason in reasons):
            return None
        score = max(float(getattr(signal, "score", 0.0) or 0.0), 0.0)
        impulse = 1.0 + min(score / 40.0, 0.25)
        return {
            "volume_impulse": impulse,
            "volume_impulse_source": "score_reasons_weak_approx",
            "volume_impulse_missing": False,
            "volume_impulse_raw": ",".join(str(reason) for reason in (getattr(signal, "reasons", []) or [])),
            "volume_baseline": None,
            "volume_current": None,
        }

    def _derive_volume_impulse(self, signal, state, buy_flow: float, sell_flow: float, override=None) -> dict[str, object]:
        meta = dict(getattr(signal, "meta", {}) or {})
        source_items = [(meta, "meta")]
        if isinstance(override, dict):
            source_items.append((dict(override), "meta.executor_snapshot"))
        for source_meta, prefix in source_items:
            derived = self._volume_impulse_from_meta(source_meta, prefix)
            if derived is not None:
                return derived

        derived = self._volume_impulse_from_baseline_meta(meta)
        if derived is not None:
            return derived

        derived = self._volume_impulse_from_state(state, buy_flow, sell_flow)
        if derived is not None:
            return derived

        derived = self._volume_impulse_from_reasons(signal)
        if derived is not None:
            return derived

        return {
            "volume_impulse": 1.0,
            "volume_impulse_source": "missing_default",
            "volume_impulse_missing": True,
            "volume_impulse_raw": None,
            "volume_baseline": None,
            "volume_current": buy_flow + sell_flow if buy_flow + sell_flow > 0 else None,
        }

    def _paper_executor_snapshot(self, signal, state=None) -> tuple[OrderflowSnapshot, bool]:
        meta = dict(getattr(signal, "meta", {}) or {})
        override = meta.get("executor_snapshot")
        if isinstance(override, dict):
            data = dict(override)
            price = self._float_or_default(data.get("price"), self._optional_float(signal.entry) or 0.0)
            buy_flow = self._float_or_default(data.get("buy_flow"), 1.0)
            sell_flow = self._float_or_default(data.get("sell_flow"), 1.0)
            volume_diagnostics = self._derive_volume_impulse(signal, state, buy_flow, sell_flow, override=data)
            self._record_volume_impulse_diagnostics(signal, volume_diagnostics)
            return (
                OrderflowSnapshot(
                    price=price,
                    spread_bps=self._float_or_default(data.get("spread_bps"), 0.0),
                    buy_flow=buy_flow,
                    sell_flow=sell_flow,
                    bid_wall_strength=self._float_or_default(data.get("bid_wall_strength"), 0.0),
                    ask_wall_strength=self._float_or_default(data.get("ask_wall_strength"), 0.0),
                    volume_impulse=float(volume_diagnostics["volume_impulse"]),
                    support=self._optional_float(data.get("support", meta.get("support"))),
                    resistance=self._optional_float(
                        data.get(
                            "resistance",
                            meta.get("resistance") or meta.get("resistance_1") or meta.get("corridor_high"),
                        )
                    ),
                    ema20=self._optional_float(data.get("ema20", meta.get("ema20"))),
                    vwap=self._optional_float(data.get("vwap", meta.get("vwap"))),
                    bars_since_entry=int(self._float_or_default(data.get("bars_since_entry"), 0.0)),
                ),
                price <= 0,
            )

        weak = state is None
        latest_book = state.snapshots[-1] if state is not None and getattr(state, "snapshots", None) else None
        price = float(getattr(latest_book, "mid", 0.0) or signal.entry or 0.0)
        spread_bps = float(getattr(latest_book, "spread_bps", 0.0) if latest_book is not None else 0.0)

        trades = list(getattr(state, "trades", []) or []) if state is not None else []
        buy_flow = sum(
            float(getattr(t, "notional", 0.0) or 0.0)
            for t in trades
            if str(getattr(t, "side", "")).lower() == "buy"
        )
        sell_flow = sum(
            float(getattr(t, "notional", 0.0) or 0.0)
            for t in trades
            if str(getattr(t, "side", "")).lower() == "sell"
        )
        if buy_flow <= 0 and sell_flow <= 0:
            weak = True
            buy_flow = sell_flow = 1.0

        bid_wall_strength = min(len(getattr(state, "bid_walls", []) or []) / 6.0, 1.0) if state is not None else 0.0
        ask_wall_strength = min(len(getattr(state, "ask_walls", []) or []) / 6.0, 1.0) if state is not None else 0.0

        support = self._optional_float(meta.get("support"))
        resistance = self._optional_float(meta.get("resistance") or meta.get("resistance_1") or meta.get("corridor_high"))
        if support is None and str(signal.side).lower() == "buy":
            support = float(signal.stop_loss or 0.0) or None
        if resistance is None and str(signal.side).lower() == "sell":
            resistance = float(signal.stop_loss or 0.0) or None

        volume_diagnostics = self._derive_volume_impulse(signal, state, buy_flow, sell_flow)
        self._record_volume_impulse_diagnostics(signal, volume_diagnostics)
        return (
            OrderflowSnapshot(
                price=price,
                spread_bps=spread_bps,
                buy_flow=buy_flow,
                sell_flow=sell_flow,
                bid_wall_strength=bid_wall_strength,
                ask_wall_strength=ask_wall_strength,
                volume_impulse=float(volume_diagnostics["volume_impulse"]),
                support=support,
                resistance=resistance,
                ema20=self._optional_float(meta.get("ema20")),
                vwap=self._optional_float(meta.get("vwap")),
                bars_since_entry=0,
            ),
            weak or price <= 0,
        )

    def _volume_impulse_report_cap_fields(
        self,
        volume_impulse: float | None,
        required_volume_impulse: float | None,
    ) -> dict[str, object]:
        cap = self.VOLUME_IMPULSE_REPORT_CAP
        if volume_impulse is None:
            return {
                "volume_impulse_capped": None,
                "volume_impulse_cap": cap,
                "volume_impulse_was_capped": False,
                "volume_impulse_ratio_to_required_capped": None,
            }

        capped = min(volume_impulse, cap)
        ratio_capped = None
        if required_volume_impulse is not None and required_volume_impulse > 0:
            ratio_capped = capped / required_volume_impulse
        return {
            "volume_impulse_capped": capped,
            "volume_impulse_cap": cap,
            "volume_impulse_was_capped": volume_impulse > cap,
            "volume_impulse_ratio_to_required_capped": ratio_capped,
        }

    @staticmethod
    def _optional_float(value) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _float_or_default(cls, value, default: float) -> float:
        parsed = cls._optional_float(value)
        return default if parsed is None else parsed

    @staticmethod
    def _normalize_fraction_or_percent(value: float) -> float:
        parsed = float(value)
        if abs(parsed) > 1.0:
            return parsed / 100.0
        return parsed

    @classmethod
    def _active_r_from_fractional_price_move(
        cls, *, entry_price: float, initial_risk: float, move: float
    ) -> float:
        if initial_risk <= 0:
            raise ValueError("initial_risk must be positive")
        normalized_move = cls._normalize_fraction_or_percent(move)
        return abs(float(entry_price) * normalized_move) / float(initial_risk)

    @classmethod
    def _active_price_extremes_from_diagnostics(
        cls, diagnostics: dict[str, object]
    ) -> tuple[float | None, float | None]:
        max_price = cls._optional_float(diagnostics.get("executor_max_price"))
        if max_price is None:
            max_price = cls._optional_float(diagnostics.get("max_price"))
        min_price = cls._optional_float(diagnostics.get("executor_min_price"))
        if min_price is None:
            min_price = cls._optional_float(diagnostics.get("min_price"))
        return max_price, min_price

    @classmethod
    def _current_unrealized_r(
        cls, *, side: str, entry_price: float, current_price: float, initial_risk: float
    ) -> float | None:
        if initial_risk <= 0:
            return None
        if side == "Sell":
            return (float(entry_price) - float(current_price)) / float(initial_risk)
        return (float(current_price) - float(entry_price)) / float(initial_risk)

    @classmethod
    def _normalize_active_r_scale(
        cls,
        *,
        side: str,
        entry_price: float,
        initial_risk: float | None,
        max_gain_r: float,
        max_drawdown_r: float,
        diagnostics_json: dict[str, object],
        current_price: float | None = None,
    ) -> tuple[float, float, dict[str, object]]:
        if initial_risk is None or initial_risk <= 0:
            return max_gain_r, max_drawdown_r, {}

        original_gain = float(max_gain_r)
        original_drawdown = float(max_drawdown_r)
        suspicious_gain = original_gain > cls.ACTIVE_R_SUSPICIOUS_THRESHOLD
        suspicious_drawdown = abs(original_drawdown) > cls.ACTIVE_R_SUSPICIOUS_THRESHOLD
        if not suspicious_gain and not suspicious_drawdown:
            if diagnostics_json.get("suspicious_active_r_scale") is True:
                return max_gain_r, max_drawdown_r, {}
            return max_gain_r, max_drawdown_r, {"suspicious_active_r_scale": False}

        recovered_gain = 0.0 if suspicious_gain else original_gain
        recovered_drawdown = 0.0 if suspicious_drawdown else original_drawdown
        recovered_from = "reset"
        current_r = None
        parsed_current_price = cls._optional_float(current_price)
        if parsed_current_price is None:
            parsed_current_price = cls._optional_float(diagnostics_json.get("price"))
        if parsed_current_price is None:
            parsed_current_price = cls._optional_float(diagnostics_json.get("current_price"))

        if parsed_current_price is not None:
            current_r = cls._current_unrealized_r(
                side=side,
                entry_price=float(entry_price),
                current_price=float(parsed_current_price),
                initial_risk=float(initial_risk),
            )
            if current_r is not None and abs(current_r) <= cls.ACTIVE_R_SUSPICIOUS_THRESHOLD:
                if suspicious_gain and current_r >= 0:
                    recovered_gain = max(float(current_r), 0.0)
                if suspicious_drawdown and current_r < 0:
                    recovered_drawdown = abs(float(current_r))
                recovered_from = "current_price"

        updates: dict[str, object] = {
            "suspicious_active_r_scale": True,
            "active_r_recovered_from": recovered_from,
            "active_r_scale_original_max_gain_r": original_gain,
            "active_r_scale_original_max_drawdown_r": original_drawdown,
        }
        if parsed_current_price is not None:
            updates["active_r_recovery_current_price"] = float(parsed_current_price)
        if current_r is not None:
            updates["active_r_recovery_current_r"] = float(current_r)

        return recovered_gain, recovered_drawdown, updates

    def _position_from_executor_row(self, signal, row) -> TradePosition:
        side = str(row["side"] or signal.side)
        entry_snapshot = self._executor_entry_snapshot_from_row(row)
        entry = float(entry_snapshot.get("executor_entry_price") or row["entry_price"] or signal.entry or 0.0)
        stop, risk_diagnostics = self._resolve_active_initial_sl(
            signal_key=str(row["signal_key"]), row=row, signal=signal, entry_price=entry, side=side
        )
        initial_risk = self._active_initial_risk(entry, stop)
        invalid_initial_risk = self._invalid_active_initial_risk(entry, stop, side)
        if invalid_initial_risk:
            initial_risk = max(abs(entry) * 0.01, 1e-9)
        raw_max_gain_r = float(row["max_gain_r"] or 0.0)
        raw_max_drawdown_r = float(row["max_drawdown_r"] or 0.0)
        max_gain_r = 0.0 if invalid_initial_risk else raw_max_gain_r
        max_drawdown_r = 0.0 if invalid_initial_risk else raw_max_drawdown_r
        active_r_diagnostics: dict[str, object] = {}
        suspicious_raw_r = (
            raw_max_gain_r > self.ACTIVE_R_SUSPICIOUS_THRESHOLD
            or abs(raw_max_drawdown_r) > self.ACTIVE_R_SUSPICIOUS_THRESHOLD
        )
        if invalid_initial_risk and suspicious_raw_r:
            active_r_diagnostics = {
                "suspicious_active_r_scale": True,
                "active_r_recovered_from": "reset",
                "active_r_scale_original_max_gain_r": raw_max_gain_r,
                "active_r_scale_original_max_drawdown_r": raw_max_drawdown_r,
            }
        if not invalid_initial_risk:
            row_diagnostics = self._parse_executor_diagnostics(
                row["diagnostics_json"] if "diagnostics_json" in row.keys() else None
            )
            max_gain_r, max_drawdown_r, active_r_diagnostics = self._normalize_active_r_scale(
                side=side,
                entry_price=entry,
                initial_risk=initial_risk,
                max_gain_r=max_gain_r,
                max_drawdown_r=max_drawdown_r,
                diagnostics_json=row_diagnostics,
                current_price=self._optional_float(row["price"]) if "price" in row.keys() else None,
            )
        self._persist_active_risk_diagnostics(
            row=row,
            entry_price=entry,
            initial_sl=stop,
            initial_risk=None if invalid_initial_risk else initial_risk,
            invalid_initial_risk=invalid_initial_risk,
            risk_diagnostics={**risk_diagnostics, **active_r_diagnostics},
            max_gain_r=max_gain_r,
            max_drawdown_r=max_drawdown_r,
        )
        if side == "Sell":
            max_price = entry + max_drawdown_r * initial_risk
            min_price = entry - max_gain_r * initial_risk
        else:
            max_price = entry + max_gain_r * initial_risk
            min_price = entry - max_drawdown_r * initial_risk
        return TradePosition(
            symbol=str(row["symbol"] or signal.symbol),
            side=side,
            state=str(row["state"] or ENTERED),
            entry_price=entry,
            stop_loss=stop,
            current_sl=float(row["current_sl"] or stop),
            max_price=max(max_price, entry),
            min_price=min(min_price, entry),
            max_gain_r=max_gain_r,
            max_drawdown_r=max_drawdown_r,
            bars_in_trade=int(row["bars_in_trade"] or 0),
            exit_price=self._optional_float(row["exit_price"]),
            exit_reason=row["exit_reason"],
            initial_risk=initial_risk,
        )

    @staticmethod
    def _active_initial_risk(entry_price: float | None, initial_sl: float | None) -> float | None:
        if entry_price is None or initial_sl is None:
            return None
        return abs(float(entry_price) - float(initial_sl))

    @classmethod
    def _invalid_active_initial_risk(cls, entry_price, initial_sl, side) -> bool:
        entry = cls._optional_float(entry_price)
        stop = cls._optional_float(initial_sl)
        if entry is None or stop is None:
            return True
        risk = abs(entry - stop)
        min_risk = max(abs(entry) * 1e-9, 1e-12)
        if risk <= min_risk:
            return True
        return cls._executor_initial_sl_invalid(side=side, entry_price=entry, initial_sl=stop)

    def _resolve_active_initial_sl(self, *, signal_key: str, row, signal, entry_price: float, side: str) -> tuple[float, dict[str, object]]:
        diagnostics = self._parse_executor_diagnostics(row["diagnostics_json"] if "diagnostics_json" in row.keys() else None)
        latest_trade = self.signal_store.get_latest_executor_trade_for_signal(signal_key)
        candidates = [
            ("executor_trades.initial_sl", latest_trade.get("initial_sl") if latest_trade is not None else None),
            ("diagnostics_json.executor_initial_sl", diagnostics.get("executor_initial_sl")),
            ("diagnostics_json.initial_sl", diagnostics.get("initial_sl")),
            ("position.executor_initial_sl", getattr(signal, "executor_initial_sl", None)),
        ]
        for source, value in candidates:
            parsed = self._optional_float(value)
            if parsed is None:
                continue
            return parsed, {"risk_basis": "initial_sl", "risk_source": source}

        diagnostic_risk = self._optional_float(diagnostics.get("initial_risk"))
        if diagnostic_risk is not None and diagnostic_risk > 0:
            stop = entry_price + diagnostic_risk if side == "Sell" else entry_price - diagnostic_risk
            return float(stop), {
                "risk_basis": "initial_risk",
                "risk_source": "diagnostics_json.initial_risk",
            }

        fallback = self._optional_float(getattr(signal, "stop_loss", None))
        if fallback is not None:
            return float(fallback), {
                "risk_basis": "initial_sl",
                "risk_source": "fallback_signal_stop_loss",
                "risk_basis_warning": "missing_executor_initial_sl",
            }

        return float(entry_price), {
            "risk_basis": "initial_sl",
            "risk_source": "missing_initial_sl",
            "risk_basis_warning": "missing_initial_sl",
        }

    def _persist_active_risk_diagnostics(
        self,
        *,
        row,
        entry_price: float,
        initial_sl: float,
        initial_risk: float | None,
        invalid_initial_risk: bool,
        risk_diagnostics: dict[str, object],
        max_gain_r: float,
        max_drawdown_r: float,
    ) -> None:
        diagnostics = self._parse_executor_diagnostics(row["diagnostics_json"] if "diagnostics_json" in row.keys() else None)
        diagnostics.update(risk_diagnostics)
        diagnostics.update(
            {
                "executor_entry_price": entry_price,
                "entry_price": entry_price,
                "executor_initial_sl": initial_sl,
                "initial_sl": initial_sl,
                "initial_risk": initial_risk,
                "invalid_initial_risk": bool(invalid_initial_risk),
            }
        )
        self.signal_store.upsert_executor_decision(
            signal_key=str(row["signal_key"]),
            symbol=str(row["symbol"]),
            side=str(row["side"]),
            state=str(row["state"]),
            action=str(row["action"]),
            reason=str(row["reason"]),
            entry_price=entry_price,
            current_sl=self._optional_float(row["current_sl"]),
            exit_price=self._optional_float(row["exit_price"]),
            exit_reason=row["exit_reason"],
            max_gain_r=max_gain_r,
            max_drawdown_r=max_drawdown_r,
            bars_in_trade=int(row["bars_in_trade"] or 0),
            price=self._optional_float(row["price"]),
            spread_bps=self._optional_float(row["spread_bps"]),
            buy_flow=self._optional_float(row["buy_flow"]),
            sell_flow=self._optional_float(row["sell_flow"]),
            required_buy_flow=self._optional_float(row["required_buy_flow"]),
            required_sell_flow=self._optional_float(row["required_sell_flow"]),
            volume_impulse=self._optional_float(row["volume_impulse"]),
            required_volume_impulse=self._optional_float(row["required_volume_impulse"]),
            bid_wall_strength=self._optional_float(row["bid_wall_strength"]),
            ask_wall_strength=self._optional_float(row["ask_wall_strength"]),
            support=self._optional_float(row["support"]),
            resistance=self._optional_float(row["resistance"]),
            ema20=self._optional_float(row["ema20"]),
            vwap=self._optional_float(row["vwap"]),
            diagnostics_json=diagnostics,
        )

    def _paper_executor_diagnostics(self, signal, snapshot=None) -> dict[str, object]:
        executor = getattr(self, "trade_executor", None)
        thresholds = {
            "max_spread_bps": self._optional_float(getattr(executor, "max_spread_bps", None)),
            "flow_ratio": self._optional_float(getattr(executor, "flow_ratio", None)),
            "min_entry_volume_impulse": self._optional_float(getattr(executor, "min_entry_volume_impulse", None)),
            "ask_wall_entry_limit": self._optional_float(getattr(executor, "ask_wall_entry_limit", None)),
            "bid_wall_entry_limit": self._optional_float(getattr(executor, "bid_wall_entry_limit", None)),
            "strong_reversal_ratio": self._optional_float(getattr(executor, "strong_reversal_ratio", None)),
            "strong_wall_exit_threshold": self._optional_float(getattr(executor, "strong_wall_exit_threshold", None)),
            "absorption_flow_ratio": self._optional_float(getattr(executor, "absorption_flow_ratio", None)),
        }
        meta = dict(getattr(signal, "meta", {}) or {})
        override = meta.get("executor_snapshot")
        values: dict[str, float | None] = {}
        for field in (
            "price",
            "spread_bps",
            "buy_flow",
            "sell_flow",
            "volume_impulse",
            "bid_wall_strength",
            "ask_wall_strength",
            "support",
            "resistance",
            "ema20",
            "vwap",
        ):
            if isinstance(override, dict):
                values[field] = self._optional_float(override.get(field)) if field in override else None
            elif snapshot is not None:
                values[field] = self._optional_float(getattr(snapshot, field, None))
            else:
                values[field] = None

        flow_ratio = thresholds["flow_ratio"]
        side = str(getattr(signal, "side", "") or "")
        required_buy_flow = None
        required_sell_flow = None
        if flow_ratio is not None:
            if side == "Buy" and values.get("sell_flow") is not None:
                required_buy_flow = values["sell_flow"] * flow_ratio
            if side == "Sell" and values.get("buy_flow") is not None:
                required_sell_flow = values["buy_flow"] * flow_ratio

        values["required_buy_flow"] = required_buy_flow
        values["required_sell_flow"] = required_sell_flow
        values["required_volume_impulse"] = thresholds["min_entry_volume_impulse"]

        volume_diagnostics = dict(meta.get("_paper_volume_impulse_diagnostics") or {})
        if volume_diagnostics and not (
            isinstance(override, dict) and volume_diagnostics.get("volume_impulse_source") == "missing_default"
        ):
            values["volume_impulse"] = self._optional_float(volume_diagnostics.get("volume_impulse"))
        required_volume = thresholds["min_entry_volume_impulse"]
        volume_impulse = self._optional_float(values.get("volume_impulse"))
        diagnostic_volume_impulse = self._optional_float(volume_diagnostics.get("volume_impulse"))
        volume_ratio_to_required = None
        if required_volume is not None and required_volume > 0:
            if volume_impulse is not None:
                volume_ratio_to_required = volume_impulse / required_volume
            elif diagnostic_volume_impulse is not None:
                volume_ratio_to_required = diagnostic_volume_impulse / required_volume

        btc_regime = str(meta.get("btc_regime") or "BTC_NEUTRAL")
        market_regime = str(meta.get("market_regime") or btc_regime)
        diagnostics_json = {
            **thresholds,
            "executor_management_policy": (
                getattr(self.trade_executor, "management_policy", MANAGEMENT_POLICY_LEGACY)
                if self.trade_executor is not None
                else MANAGEMENT_POLICY_LEGACY
            ),
            "btc_regime": btc_regime,
            "market_regime": market_regime,
            "volume_impulse_source": volume_diagnostics.get("volume_impulse_source"),
            "volume_impulse_missing": bool(volume_diagnostics.get("volume_impulse_missing", False)),
            "volume_impulse_raw": volume_diagnostics.get("volume_impulse_raw"),
            "volume_baseline": self._optional_float(volume_diagnostics.get("volume_baseline")),
            "volume_current": self._optional_float(volume_diagnostics.get("volume_current")),
            "volume_impulse_ratio_to_required": volume_ratio_to_required,
            "signal_kind": str(getattr(signal, "kind", "") or ""),
            **self._volume_impulse_report_cap_fields(volume_impulse, required_volume),
        }
        values["diagnostics_json"] = diagnostics_json
        return values

    @classmethod
    def _is_terminal_executor_outcome(cls, row) -> bool:
        if row is None:
            return False
        try:
            state = str(row["state"] or "").strip().upper()
        except (KeyError, IndexError):
            state = ""
        try:
            action = str(row["action"] or "").strip().upper()
        except (KeyError, IndexError):
            action = ""
        return state in cls.TERMINAL_EXECUTOR_OUTCOME_STATES or action in cls.TERMINAL_EXECUTOR_OUTCOME_ACTIONS

    @staticmethod
    def _executor_row_value(row, key: str):
        if row is None:
            return None
        try:
            return row[key]
        except (KeyError, IndexError):
            return None

    @classmethod
    def _annotate_terminal_executor_attempt(cls, diagnostics: dict[str, object], previous_row) -> None:
        previous_terminal = cls._is_terminal_executor_outcome(previous_row)
        diagnostics["previous_terminal_outcome_reused"] = False
        diagnostics["new_executor_attempt_after_terminal"] = bool(previous_terminal)
        diagnostics["previous_terminal_state"] = cls._executor_row_value(previous_row, "state") if previous_terminal else None
        diagnostics["previous_terminal_reason"] = cls._executor_row_value(previous_row, "reason") if previous_terminal else None
        diagnostics["previous_terminal_updated_at"] = cls._executor_row_value(previous_row, "updated_at") if previous_terminal else None

    @staticmethod
    def _testnet_trade_key(signal_key: str) -> str:
        return f"testnet|{signal_key}"

    def _apply_testnet_diagnostics(self, diagnostics_json: dict[str, object], result: dict[str, object] | None) -> None:
        mode = getattr(self, "trade_executor_mode", "paper")
        if self._normalize_trade_executor_mode(mode) != "testnet":
            return
        result = result or {}
        diagnostics_json.update(
            {
                "trade_executor_mode": "testnet",
                "testnet_order_attempted": bool(result.get("status") in {"placed", "failed"}),
                "testnet_order_status": result.get("status") or "not_attempted",
                "testnet_order_id": result.get("order_id"),
                "notional_usdt": self._optional_float(result.get("notional_usdt")),
                "qty": self._optional_float(result.get("qty")),
                "testnet_blocked_reason": result.get("reason") if not result.get("ok") else None,
            }
        )

    def _execute_testnet_entry(self, signal_key: str, signal, snapshot: OrderflowSnapshot) -> dict[str, object]:
        executor = getattr(self, "testnet_order_executor", None)
        if executor is None:
            return {"ok": False, "status": "blocked", "reason": "entry_blocked_testnet_executor_missing"}
        return executor.place_entry_order(
            signal_key=signal_key,
            trade_key=self._testnet_trade_key(signal_key),
            symbol=str(signal.symbol),
            price=float(snapshot.price),
        )

    def _execute_testnet_exit(self, signal_key: str, signal, snapshot: OrderflowSnapshot | None) -> dict[str, object]:
        executor = getattr(self, "testnet_order_executor", None)
        if executor is None:
            return {"ok": False, "status": "blocked", "reason": "exit_blocked_testnet_executor_missing"}
        price = float(snapshot.price) if snapshot is not None else float(signal.entry)
        self.logger.info("Testnet exit attempt symbol=%s signal_key=%s price=%s", signal.symbol, signal_key, price)
        result = executor.place_exit_order(
            signal_key=signal_key,
            trade_key=self._testnet_trade_key(signal_key),
            symbol=str(signal.symbol),
            price=price,
        )
        self.logger.info(
            "Testnet exit result symbol=%s signal_key=%s status=%s order_id=%s reason=%s",
            signal.symbol,
            signal_key,
            result.get("status"),
            result.get("order_id"),
            result.get("reason"),
        )
        return result

    def _store_paper_executor_decision(
        self,
        signal_key: str,
        signal,
        decision,
        position=None,
        snapshot=None,
        setup=None,
        testnet_result=None,
        observation_context=None,
    ):
        previous_row = self.signal_store.get_executor_outcome(signal_key)
        diagnostics = self._paper_executor_diagnostics(signal, snapshot)
        diagnostics_json = diagnostics.get("diagnostics_json")
        if not isinstance(diagnostics_json, dict):
            diagnostics_json = self._parse_executor_diagnostics(diagnostics_json)
            diagnostics["diagnostics_json"] = diagnostics_json
        if observation_context is None:
            observation_context = self._testnet_observation_entry_context(signal, None)
        diagnostics_json.update(dict(observation_context))
        is_new_entry = position is not None and str(decision.action) in {ENTER_LONG, ENTER_SHORT}
        previous_terminal = self._is_terminal_executor_outcome(previous_row)
        if not previous_terminal:
            self._preserve_executor_entry_diagnostics(diagnostics_json, previous_row, preserve_breakeven_time=not is_new_entry)
        self._annotate_terminal_executor_attempt(diagnostics_json, previous_row)
        if is_new_entry:
            diagnostics_json.pop("breakeven_time", None)
            diagnostics_json.update(
                {
                    "executor_entry_time": datetime.now(UTC).isoformat(),
                    "executor_entry_price": float(position.entry_price),
                    "executor_initial_sl": float(position.stop_loss),
                    "initial_sl": float(position.stop_loss),
                    "initial_risk": float(position.initial_risk),
                    "risk_basis": "initial_sl",
                    "executor_side": str(position.side),
                    "executor_signal_key": signal_key,
                    "executor_timeframe": str(signal.meta.get("tf") or "1"),
                }
            )
        elif str(decision.action) == MOVE_SL_TO_BREAKEVEN and not diagnostics_json.get("breakeven_time"):
            diagnostics_json["breakeven_time"] = datetime.now(UTC).isoformat()
        if setup is not None and snapshot is not None and self.trade_executor is not None:
            diagnostics_json.update(self.trade_executor.entry_gate_diagnostics(setup, snapshot))
        if str(decision.reason) == "entry_blocked_market_regime":
            btc_regime = str(getattr(signal, "meta", {}).get("btc_regime") or "BTC_NEUTRAL")
            market_regime = str(getattr(signal, "meta", {}).get("market_regime") or btc_regime)
            diagnostics_json.update(
                {
                    "btc_regime": btc_regime,
                    "market_regime": market_regime,
                    "market_regime_blocked": True,
                    "market_regime_reason": "entry_blocked_market_regime",
                }
            )
        if str(decision.reason) == ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION:
            if setup is None:
                setup = self._paper_executor_setup(signal)
            if snapshot is not None and self.trade_executor is not None:
                diagnostics_json.update(self.trade_executor.absorption_gate_diagnostics(setup, snapshot))
            diagnostics_json["absorption_gate_reason"] = ENTRY_BLOCKED_ABSORPTION_WEAK_CONFIRMATION

        if (
            str(decision.reason) == "entry_blocked_volume_impulse"
            and diagnostics_json.get("volume_impulse_source") == "missing_default"
        ):
            diagnostics_json["blocker_root_cause"] = "missing_volume_impulse_mapping"
        self._apply_testnet_diagnostics(diagnostics_json, testnet_result)
        self._apply_executor_exit_shadow(
            signal_key=signal_key,
            signal=signal,
            position=position,
            snapshot=snapshot,
            diagnostics_json=diagnostics_json,
            previous_row=previous_row,
        )
        max_gain_r = float(position.max_gain_r) if position is not None else 0.0
        max_drawdown_r = float(position.max_drawdown_r) if position is not None else 0.0
        if position is not None:
            diagnostics_json.update(
                {
                    "executor_max_price": float(position.max_price),
                    "executor_min_price": float(position.min_price),
                }
            )
        row = self.signal_store.upsert_executor_decision(
            signal_key=signal_key,
            symbol=str(signal.symbol),
            side=str(signal.side),
            state=str(decision.next_state),
            action=str(decision.action),
            reason=str(decision.reason),
            entry_price=float(position.entry_price) if position is not None else self._optional_float(signal.entry),
            current_sl=float(position.current_sl) if position is not None else self._optional_float(signal.stop_loss),
            exit_price=float(position.exit_price) if position is not None and position.exit_price is not None else None,
            exit_reason=position.exit_reason if position is not None else None,
            max_gain_r=max_gain_r,
            max_drawdown_r=max_drawdown_r,
            bars_in_trade=int(position.bars_in_trade) if position is not None else 0,
            **diagnostics,
        )
        self.logger.info(
            "Paper executor decision symbol=%s side=%s action=%s reason=%s state=%s max_gain_r=%.4f max_drawdown_r=%.4f",
            signal.symbol,
            signal.side,
            row["action"],
            row["reason"],
            row["state"],
            float(row["max_gain_r"] or 0.0),
            float(row["max_drawdown_r"] or 0.0),
        )
        if str(row["action"]) == WATCH and snapshot is not None:
            status = None
            if isinstance(observation_context, dict):
                status = observation_context.get("original_signal_status")
            self._record_signal_forward_outcome(
                signal_key,
                signal,
                status=str(status or ""),
                snapshot=snapshot,
                executor_block_reason=str(row["reason"] or ""),
            )
        if str(row["action"]) == EXIT:
            mode = self._normalize_trade_executor_mode(getattr(self, "trade_executor_mode", "paper"))
            if mode == "testnet":
                exit_result = self._execute_testnet_exit(signal_key, signal, snapshot)
                diagnostics_json = self._parse_executor_diagnostics(row["diagnostics_json"])
                self._apply_testnet_diagnostics(diagnostics_json, exit_result)
                row = self.signal_store.upsert_executor_decision(
                    signal_key=signal_key,
                    symbol=str(signal.symbol),
                    side=str(signal.side),
                    state=str(decision.next_state),
                    action=str(decision.action),
                    reason=str(decision.reason),
                    entry_price=float(position.entry_price) if position is not None else self._optional_float(signal.entry),
                    current_sl=float(position.current_sl) if position is not None else self._optional_float(signal.stop_loss),
                    exit_price=float(position.exit_price) if position is not None and position.exit_price is not None else None,
                    exit_reason=position.exit_reason if position is not None else None,
                    max_gain_r=float(row["max_gain_r"] or 0.0),
                    max_drawdown_r=float(row["max_drawdown_r"] or 0.0),
                    bars_in_trade=int(position.bars_in_trade) if position is not None else 0,
                    diagnostics_json=diagnostics_json,
                )
            self._best_effort_store_executor_trade(signal_key, signal, decision, position, row, previous_row, diagnostics_json)

        trade_learning = getattr(self, "trade_learning", None)

        if trade_learning is not None:
            trade_learning.record_executor_decision(
                signal=signal,
                signal_key=signal_key,
                state=str(row["state"]),
                action=str(row["action"]),
                reason=str(row["reason"]),
                price=self._optional_float(row["exit_price"]) or self._optional_float(row["entry_price"]),
                features={
                    "current_sl": self._optional_float(row["current_sl"]),
                    "exit_price": self._optional_float(row["exit_price"]),
                    "exit_reason": row["exit_reason"],
                    "max_gain_r": float(row["max_gain_r"] or 0.0),
                    "max_drawdown_r": float(row["max_drawdown_r"] or 0.0),
                    "bars_in_trade": int(row["bars_in_trade"] or 0),
                    "volume_impulse": self._optional_float(row["volume_impulse"]),
                    "required_volume_impulse": self._optional_float(row["required_volume_impulse"]),
                    "buy_flow": self._optional_float(row["buy_flow"]),
                    "sell_flow": self._optional_float(row["sell_flow"]),
                    "required_buy_flow": self._optional_float(row["required_buy_flow"]),
                    "required_sell_flow": self._optional_float(row["required_sell_flow"]),
                    "spread_bps": self._optional_float(row["spread_bps"]),
                    "ask_wall_strength": self._optional_float(row["ask_wall_strength"]),
                    "bid_wall_strength": self._optional_float(row["bid_wall_strength"]),
                    "volume_impulse_source": diagnostics_json.get("volume_impulse_source") if isinstance(diagnostics_json, dict) else None,
                    "volume_impulse_missing": diagnostics_json.get("volume_impulse_missing") if isinstance(diagnostics_json, dict) else None,
                    "volume_impulse_raw": diagnostics_json.get("volume_impulse_raw") if isinstance(diagnostics_json, dict) else None,
                    "volume_baseline": diagnostics_json.get("volume_baseline") if isinstance(diagnostics_json, dict) else None,
                    "volume_current": diagnostics_json.get("volume_current") if isinstance(diagnostics_json, dict) else None,
                    "volume_impulse_ratio_to_required": diagnostics_json.get("volume_impulse_ratio_to_required") if isinstance(diagnostics_json, dict) else None,
                    "blocker_root_cause": diagnostics_json.get("blocker_root_cause") if isinstance(diagnostics_json, dict) else None,
                    "btc_regime": diagnostics_json.get("btc_regime") if isinstance(diagnostics_json, dict) else None,
                    "market_regime": diagnostics_json.get("market_regime") if isinstance(diagnostics_json, dict) else None,
                    "market_regime_blocked": diagnostics_json.get("market_regime_blocked") if isinstance(diagnostics_json, dict) else None,
                    "market_regime_reason": diagnostics_json.get("market_regime_reason") if isinstance(diagnostics_json, dict) else None,
                },
            )

        return row


    @staticmethod
    def _parse_executor_time(value) -> datetime | None:
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

    @classmethod
    def _duration_minutes(cls, entry_time, exit_time) -> float | None:
        entry_dt = cls._parse_executor_time(entry_time)
        exit_dt = cls._parse_executor_time(exit_time)
        if entry_dt is None or exit_dt is None:
            return None
        return max((exit_dt - entry_dt).total_seconds() / 60.0, 0.0)

    @staticmethod
    def _parse_executor_diagnostics(value) -> dict[str, object]:
        if isinstance(value, dict):
            return dict(value)
        if value in (None, ""):
            return {}
        try:
            parsed = json.loads(str(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}

    @classmethod
    def _executor_entry_snapshot_from_row(cls, row) -> dict[str, object]:
        if row is None:
            return {}
        try:
            diagnostics = cls._parse_executor_diagnostics(row["diagnostics_json"])
        except (KeyError, IndexError):
            return {}
        return cls._executor_entry_snapshot_from_diagnostics(diagnostics)

    @classmethod
    def _executor_entry_snapshot_from_diagnostics(
        cls, diagnostics: dict[str, object], *, include_breakeven_time: bool = True
    ) -> dict[str, object]:
        keys = [
            "executor_entry_time",
            "executor_entry_price",
            "executor_initial_sl",
            "initial_sl",
            "initial_risk",
            "risk_basis",
            "risk_basis_warning",
            "invalid_initial_risk",
            "executor_side",
            "executor_signal_key",
            "executor_timeframe",
        ]
        if include_breakeven_time:
            keys.append("breakeven_time")
        return {key: diagnostics.get(key) for key in keys if diagnostics.get(key) not in (None, "")}

    @classmethod
    def _preserve_executor_entry_diagnostics(
        cls, diagnostics: dict[str, object], previous_row, *, preserve_breakeven_time: bool = True
    ) -> None:
        """Preserve executor entry snapshot fields across HOLD/BREAKEVEN/EXIT diagnostic rewrites."""
        if previous_row is None:
            snapshot = {}
        else:
            try:
                previous_diagnostics = cls._parse_executor_diagnostics(previous_row["diagnostics_json"])
            except (KeyError, IndexError):
                previous_diagnostics = {}
            snapshot = cls._executor_entry_snapshot_from_diagnostics(
                previous_diagnostics, include_breakeven_time=preserve_breakeven_time
            )
        for key, value in snapshot.items():
            if diagnostics.get(key) in (None, ""):
                diagnostics[key] = value

    @staticmethod
    def _executor_shadow_snapshot_from_diagnostics(diagnostics: dict[str, object]) -> dict[str, object]:
        keys = [
            "exit_shadow_enabled",
            "exit_shadow_policy",
            "exit_shadow_peak_r",
            "exit_shadow_floor_r",
            "exit_shadow_current_r",
            "exit_shadow_triggered",
            "exit_shadow_triggered_at",
            "exit_shadow_exit_r",
            "exit_shadow_exit_reason",
            "exit_shadow_delta_vs_actual_open_r",
        ]
        return {key: diagnostics.get(key) for key in keys if key in diagnostics}

    def _apply_executor_exit_shadow(
        self,
        *,
        signal_key: str,
        signal,
        position,
        snapshot,
        diagnostics_json: dict[str, object],
        previous_row,
    ) -> None:
        shadow_enabled = bool(getattr(self, "executor_exit_shadow_enabled", False))
        shadow_policy = str(getattr(self, "executor_exit_shadow_policy", DEFAULT_EXIT_SHADOW_POLICY) or DEFAULT_EXIT_SHADOW_POLICY)
        diagnostics_json["exit_shadow_enabled"] = shadow_enabled
        diagnostics_json["exit_shadow_policy"] = shadow_policy
        if not shadow_enabled or position is None or snapshot is None:
            return

        previous_diagnostics = {}
        if previous_row is not None:
            previous_diagnostics = self._parse_executor_diagnostics(previous_row["diagnostics_json"])
        entry_price = self._optional_float(diagnostics_json.get("executor_entry_price"))
        if entry_price is None:
            entry_price = self._optional_float(getattr(position, "entry_price", None))
        initial_sl = self._optional_float(diagnostics_json.get("executor_initial_sl"))
        if initial_sl is None:
            initial_sl = self._optional_float(getattr(position, "stop_loss", None))
        current_price = self._optional_float(getattr(snapshot, "price", None))
        side = str(diagnostics_json.get("executor_side") or getattr(position, "side", ""))
        if entry_price is None or initial_sl is None or current_price is None:
            return
        current_r = current_unrealized_r(
            side=side,
            current_price=current_price,
            entry_price=entry_price,
            initial_sl=initial_sl,
        )
        if current_r is None:
            return

        evaluation = evaluate_exit_shadow_policy(
            policy=shadow_policy,
            previous_peak_r=self._optional_float(previous_diagnostics.get("exit_shadow_peak_r")),
            observed_max_gain_r=self._optional_float(getattr(position, "max_gain_r", None)),
            current_r=current_r,
        )
        previously_triggered_at = previous_diagnostics.get("exit_shadow_triggered_at")
        triggered_at = previously_triggered_at
        first_trigger = bool(evaluation.triggered and not triggered_at)
        if first_trigger:
            triggered_at = utc_now_iso()

        diagnostics_json.update(
            {
                "exit_shadow_enabled": True,
                "exit_shadow_policy": evaluation.policy,
                "exit_shadow_peak_r": evaluation.peak_r,
                "exit_shadow_floor_r": evaluation.floor_r,
                "exit_shadow_current_r": evaluation.current_r,
                "exit_shadow_triggered": bool(evaluation.triggered or previously_triggered_at),
                "exit_shadow_triggered_at": triggered_at,
                "exit_shadow_exit_r": self._optional_float(
                    evaluation.exit_r if evaluation.exit_r is not None else previous_diagnostics.get("exit_shadow_exit_r")
                ),
                "exit_shadow_exit_reason": evaluation.exit_reason or previous_diagnostics.get("exit_shadow_exit_reason"),
                "exit_shadow_delta_vs_actual_open_r": (
                    self._optional_float(evaluation.exit_r if evaluation.exit_r is not None else previous_diagnostics.get("exit_shadow_exit_r")) - current_r
                    if self._optional_float(evaluation.exit_r if evaluation.exit_r is not None else previous_diagnostics.get("exit_shadow_exit_r")) is not None
                    else None
                ),
            }
        )
        if first_trigger:
            self.signal_store.add_trade_lifecycle_event(
                {
                    "signal_key": signal_key,
                    "symbol": str(getattr(signal, "symbol", "UNKNOWN")),
                    "timeframe": str(getattr(signal, "meta", {}).get("tf") or diagnostics_json.get("executor_timeframe") or ""),
                    "side": side,
                    "event_type": "EXECUTOR_SHADOW_EXIT",
                    "status": "SHADOW_EXIT",
                    "action": "SHADOW_TRAILING_EXIT",
                    "reason": evaluation.exit_reason or "shadow_trailing_40pct_after_1r_triggered",
                    "price": current_price,
                    "score": self._optional_float(getattr(signal, "score", None)),
                    "btc_regime": str(getattr(signal, "meta", {}).get("btc_regime") or "") or None,
                    "market_regime": str(getattr(signal, "meta", {}).get("market_regime") or "") or None,
                    "features": self._executor_shadow_snapshot_from_diagnostics(diagnostics_json),
                    "created_at": triggered_at,
                }
            )

    def _executor_entry_snapshot_from_lifecycle(self, signal_key: str, exit_time) -> dict[str, object]:
        events = self.signal_store.get_trade_lifecycle_events(signal_key)
        exit_dt = self._parse_executor_time(exit_time)
        enter_events = []
        for event in events:
            if str(event.get("event_type")) != "EXECUTOR_ENTER":
                continue
            created_at = event.get("created_at")
            event_dt = self._parse_executor_time(created_at)
            if exit_dt is not None and event_dt is not None and event_dt > exit_dt:
                continue
            enter_events.append(event)
        if not enter_events:
            return {}
        event = enter_events[-1]
        snapshot = {
            "executor_entry_time": event.get("created_at"),
            "executor_entry_price": event.get("price"),
            "executor_side": event.get("side"),
            "executor_signal_key": signal_key,
            "executor_timeframe": event.get("timeframe"),
        }
        return {key: value for key, value in snapshot.items() if value not in (None, "")}

    @classmethod
    def _executor_initial_sl_invalid(cls, *, side, entry_price, initial_sl) -> bool:
        entry = cls._optional_float(entry_price)
        stop = cls._optional_float(initial_sl)
        if entry is None or stop is None:
            return False
        if str(side) == "Sell":
            return stop <= entry
        return stop >= entry

    @classmethod
    def _executor_r_result(cls, *, side, entry_price, exit_price, initial_sl, current_sl) -> float | None:
        entry = cls._optional_float(entry_price)
        exit_value = cls._optional_float(exit_price)
        stop = cls._optional_float(initial_sl)
        if entry is None or exit_value is None or stop is None:
            return None
        if cls._executor_initial_sl_invalid(side=side, entry_price=entry, initial_sl=stop):
            return None
        risk = abs(entry - stop)
        if risk <= 0:
            return None
        if str(side) == "Sell":
            return (entry - exit_value) / risk
        return (exit_value - entry) / risk

    @staticmethod
    def _stable_executor_trade_key(signal_key: str, entry_time, exit_time, exit_price, exit_reason) -> str:
        parts = [signal_key, str(entry_time or "no_entry_time"), str(exit_time or "no_exit_time")]
        if not exit_time:
            parts.extend([str(exit_price or "no_exit_price"), str(exit_reason or "no_exit_reason")])
        return "|".join(parts)

    def _best_effort_store_executor_trade(
        self,
        signal_key: str,
        signal,
        decision,
        position,
        row,
        previous_row,
        diagnostics_json,
    ) -> None:
        try:
            diagnostics_payload = self._parse_executor_diagnostics(diagnostics_json)
            for snapshot in (
                self._executor_entry_snapshot_from_row(row),
                self._executor_entry_snapshot_from_row(previous_row),
            ):
                for key, value in snapshot.items():
                    if diagnostics_payload.get(key) in (None, ""):
                        diagnostics_payload[key] = value
            exit_time = row["updated_at"]
            if not diagnostics_payload.get("executor_entry_time") or not diagnostics_payload.get("executor_entry_price"):
                lifecycle_snapshot = self._executor_entry_snapshot_from_lifecycle(signal_key, exit_time)
                for key, value in lifecycle_snapshot.items():
                    if diagnostics_payload.get(key) in (None, ""):
                        diagnostics_payload[key] = value
            entry_time = diagnostics_payload.get("executor_entry_time") or (
                previous_row["created_at"] if previous_row is not None else row["created_at"]
            )
            current_sl = self._optional_float(row["current_sl"])
            observed_exit_price = self._optional_float(row["exit_price"])
            exit_reason = row["exit_reason"] or decision.reason
            exit_price = observed_exit_price
            entry_price = self._optional_float(diagnostics_payload.get("executor_entry_price"))
            if entry_price is None:
                entry_price = self._optional_float(row["entry_price"])
            side = str(diagnostics_payload.get("executor_side") or row["side"])
            initial_sl = self._optional_float(diagnostics_payload.get("executor_initial_sl"))
            if initial_sl is None:
                fallback_sl = self._optional_float(position.stop_loss) if position is not None else current_sl
                if not self._executor_initial_sl_invalid(side=side, entry_price=entry_price, initial_sl=fallback_sl):
                    initial_sl = fallback_sl
                    diagnostics_payload["executor_initial_sl"] = initial_sl
            if str(exit_reason) == "exit_stop_loss_hit":
                final_sl = self._optional_float(diagnostics_payload.get("final_sl"))
                if final_sl is None and position is not None:
                    final_sl = self._optional_float(getattr(position, "current_sl", None))
                effective_stop_price = next(
                    (
                        stop_price
                        for stop_price in (current_sl, final_sl, initial_sl)
                        if stop_price is not None
                    ),
                    None,
                )
                if effective_stop_price is not None:
                    diagnostics_payload["observed_exit_price"] = observed_exit_price
                    diagnostics_payload["stop_execution_price"] = effective_stop_price
                    exit_price = effective_stop_price

            invalid_initial_sl = self._executor_initial_sl_invalid(side=side, entry_price=entry_price, initial_sl=initial_sl)
            diagnostics_payload["invalid_initial_sl"] = bool(invalid_initial_sl)
            if invalid_initial_sl:
                self.logger.debug(
                    "Executor trade %s has invalid_initial_sl side=%s entry_price=%s initial_sl=%s",
                    signal_key,
                    side,
                    entry_price,
                    initial_sl,
                )
            r_result = self._executor_r_result(
                side=side,
                entry_price=entry_price,
                exit_price=exit_price,
                initial_sl=initial_sl,
                current_sl=current_sl,
            )
            # A breakeven flag is valid only when the stop is actually at or beyond entry.
            # Do not trust state/action alone, because restored paper rows can carry
            # PROTECT_BREAKEVEN/TRAILING_PROFIT while current_sl is still below entry.
            protected_by_stop = False
            if entry_price is not None and current_sl is not None:
                if side == "Buy":
                    protected_by_stop = current_sl >= entry_price
                elif side == "Sell":
                    protected_by_stop = current_sl <= entry_price

            moved_to_breakeven = bool(protected_by_stop)
            breakeven_time = None
            if moved_to_breakeven:
                events = self.signal_store.get_trade_lifecycle_events(signal_key)
                breakeven_event = next(
                    (
                        event
                        for event in events
                        if str(event.get("action")) == MOVE_SL_TO_BREAKEVEN
                        or str(event.get("status")) in {PROTECT_BREAKEVEN, TRAILING_PROFIT}
                    ),
                    None,
                )
                breakeven_time = diagnostics_payload.get("breakeven_time")
                if breakeven_time is None:
                    breakeven_time = breakeven_event.get("created_at") if breakeven_event else None
                if breakeven_time is None and previous_row is not None and str(previous_row["action"]) == MOVE_SL_TO_BREAKEVEN:
                    breakeven_time = previous_row["updated_at"]
                if breakeven_time is not None:
                    diagnostics_payload["breakeven_time"] = breakeven_time

            entry_action = ENTER_SHORT if str(row["side"]) == "Sell" else ENTER_LONG
            if previous_row is not None and str(previous_row["action"]) in {ENTER_LONG, ENTER_SHORT}:
                entry_action = str(previous_row["action"])

            shadow_exit_r = self._optional_float(diagnostics_payload.get("exit_shadow_exit_r"))
            diagnostics_payload["exit_shadow_policy"] = diagnostics_payload.get("exit_shadow_policy")
            diagnostics_payload["exit_shadow_peak_r"] = self._optional_float(diagnostics_payload.get("exit_shadow_peak_r"))
            diagnostics_payload["exit_shadow_floor_r"] = self._optional_float(diagnostics_payload.get("exit_shadow_floor_r"))
            diagnostics_payload["exit_shadow_triggered"] = bool(diagnostics_payload.get("exit_shadow_triggered"))
            diagnostics_payload["exit_shadow_triggered_at"] = diagnostics_payload.get("exit_shadow_triggered_at")
            diagnostics_payload["exit_shadow_exit_r"] = shadow_exit_r
            diagnostics_payload["exit_shadow_exit_reason"] = diagnostics_payload.get("exit_shadow_exit_reason")
            diagnostics_payload["exit_shadow_actual_r"] = r_result
            diagnostics_payload["exit_shadow_delta_r"] = (shadow_exit_r - r_result) if shadow_exit_r is not None and r_result is not None else None

            trade_key = self._stable_executor_trade_key(signal_key, entry_time, exit_time, exit_price, exit_reason)
            timeframe = str(diagnostics_payload.get("executor_timeframe") or signal.meta.get("tf") or "1")
            max_gain_r = self._optional_float(row["max_gain_r"])
            max_drawdown_r = self._optional_float(row["max_drawdown_r"])
            self.signal_store.upsert_executor_trade(
                {
                    "trade_key": trade_key,
                    "signal_key": signal_key,
                    "symbol": str(signal.symbol),
                    "timeframe": timeframe,
                    "side": side,
                    "state": str(row["state"]),
                    "entry_action": entry_action,
                    "exit_action": str(row["action"]),
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "initial_sl": initial_sl,
                    "final_sl": current_sl,
                    "current_sl": current_sl,
                    "entry_time": entry_time,
                    "exit_time": exit_time,
                    "exit_reason": exit_reason,
                    "r_result": r_result,
                    "max_gain_r": max_gain_r,
                    "max_drawdown_r": max_drawdown_r,
                    "bars_in_trade": int(row["bars_in_trade"] or 0),
                    "duration_minutes": self._duration_minutes(entry_time, exit_time),
                    "moved_to_breakeven": moved_to_breakeven,
                    "breakeven_time": breakeven_time,
                    "diagnostics_json": diagnostics_payload,
                    "created_at": entry_time,
                    "updated_at": exit_time,
                }
            )
            if str(exit_reason) == "exit_stop_loss_hit":
                self.signal_store.upsert_stop_loss_diagnosis(
                    {
                        "trade_key": trade_key,
                        "signal_key": signal_key,
                        "symbol": str(signal.symbol),
                        "timeframe": timeframe,
                        "side": side,
                        "entry_price": entry_price,
                        "initial_sl": initial_sl,
                        "exit_price": exit_price,
                        "exit_time": exit_time,
                        "r_result": r_result,
                        "max_gain_r": max_gain_r,
                        "max_drawdown_r": max_drawdown_r,
                        "btc_regime": diagnostics_payload.get("btc_regime") or getattr(signal, "meta", {}).get("btc_regime"),
                        "market_regime": diagnostics_payload.get("market_regime") or getattr(signal, "meta", {}).get("market_regime"),
                        "signal_kind": diagnostics_payload.get("signal_kind") or getattr(signal, "kind", None),
                        "features": {
                            **diagnostics_payload,
                            "entry_time": entry_time,
                            "bars_in_trade": int(row["bars_in_trade"] or 0),
                            "duration_minutes": self._duration_minutes(entry_time, exit_time),
                            "post_stop_observation_pending": True,
                            "post_stop_check_after_bars": [3, 6, 12, 24],
                        },
                        "post_stop_observation_pending": True,
                        "post_stop_check_after_bars": [3, 6, 12, 24],
                        "created_at": exit_time,
                        "updated_at": exit_time,
                    }
                )
        except Exception:
            self.logger.exception("Failed to write executor_trades row for %s", signal_key)

    def _parse_executor_dt(self, value) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _executor_no_progress_timeout_minutes(self, signal_key: str, signal) -> float:
        raw = os.getenv(
            "EXECUTOR_NO_PROGRESS_TIMEOUT_MINUTES_BY_TF",
            "1:20,5:40,15:75,60:180,1h:180,4h:480",
        )
        mapping: dict[str, float] = {}
        for item in raw.split(","):
            if ":" not in item:
                continue
            key, value = item.split(":", 1)
            try:
                mapping[key.strip().lower()] = float(value.strip())
            except ValueError:
                continue

        parts = str(signal_key or "").split("|")
        tf = str(getattr(signal, "timeframe", "") or "").strip().lower()
        if not tf and len(parts) > 2:
            tf = str(parts[2] or "").strip().lower()
        tf = tf or str(getattr(signal, "meta", {}).get("tf", "") or "").strip().lower()

        return mapping.get(tf, self._env_float("EXECUTOR_NO_PROGRESS_TIMEOUT_MINUTES", 40.0))

    def _executor_current_r(self, side: str, entry_price: float, price: float, initial_sl: float) -> float | None:
        risk = abs(float(entry_price) - float(initial_sl))
        if risk <= 0:
            return None
        if side == "Buy":
            return (float(price) - float(entry_price)) / risk
        if side == "Sell":
            return (float(entry_price) - float(price)) / risk
        return None

    def _apply_no_progress_timeout_exit(self, signal_key, signal, position, snapshot, decision, row):
        if not self._env_bool("EXECUTOR_NO_PROGRESS_EXIT_ENABLED", True):
            return decision

        if str(decision.action) != "HOLD" or decision.position is None:
            return decision

        updated_position = decision.position

        # Не трогаем сделки, которые уже защищены step-lock/trailing.
        if str(updated_position.state) in {"PROTECT_BREAKEVEN", "TRAILING_PROFIT", "EXITED"}:
            return decision

        target_r = self._env_float("EXECUTOR_NO_PROGRESS_TARGET_R", 0.25)
        if float(updated_position.max_gain_r or 0.0) >= target_r:
            return decision

        diagnostics = self._parse_executor_diagnostics(row["diagnostics_json"] if "diagnostics_json" in row.keys() else None)

        entry_time = (
            diagnostics.get("executor_entry_time")
            or diagnostics.get("entry_time")
            or row["created_at"]
        )
        entry_dt = self._parse_executor_dt(entry_time)
        if entry_dt is None:
            return decision

        elapsed_minutes = (datetime.now(timezone.utc) - entry_dt).total_seconds() / 60.0
        timeout_minutes = self._executor_no_progress_timeout_minutes(signal_key, signal)
        if elapsed_minutes < timeout_minutes:
            return decision

        entry_price = self._optional_float(diagnostics.get("executor_entry_price")) or self._optional_float(row["entry_price"])
        initial_sl = (
            self._optional_float(diagnostics.get("executor_initial_sl"))
            or self._optional_float(diagnostics.get("initial_sl"))
            or self._optional_float(row["current_sl"])
        )
        price = float(snapshot.price)
        side = str(updated_position.side)

        if entry_price is None or initial_sl is None:
            return decision

        current_r = self._executor_current_r(side, entry_price, price, initial_sl)
        if current_r is None:
            return decision

        max_current_r = self._env_float("EXECUTOR_NO_PROGRESS_MAX_CURRENT_R", 0.05)
        if current_r > max_current_r:
            return decision

        if self._env_bool("EXECUTOR_NO_PROGRESS_REQUIRE_FLOW_FADE", True):
            flow_ratio = self._env_float("EXECUTOR_NO_PROGRESS_FLOW_RATIO", 1.0)
            if side == "Buy":
                flow_faded = snapshot.buy_flow <= snapshot.sell_flow * flow_ratio or price <= entry_price
            elif side == "Sell":
                flow_faded = snapshot.sell_flow <= snapshot.buy_flow * flow_ratio or price >= entry_price
            else:
                flow_faded = True

            if not flow_faded:
                return decision

        exit_position = dataclasses.replace(
            updated_position,
            state="EXITED",
            exit_price=price,
            exit_reason="exit_no_progress_timeout",
        )

        return TradeDecision(
            "EXIT",
            "exit_no_progress_timeout",
            "EXITED",
            exit_position,
        )

    def _executor_signal_from_outcome_row(self, row):
        signal_key = str(row["signal_key"])
        parts = signal_key.split("|")
        market = parts[1] if len(parts) > 1 and parts[1] else "linear"
        timeframe = parts[2] if len(parts) > 2 and parts[2] else "1"
        kind = parts[3] if len(parts) > 3 and parts[3] else "CONFIRMED_LONG"
        side = str(row["side"] or (parts[4] if len(parts) > 4 else "Buy"))
        diagnostics = self._parse_executor_diagnostics(row["diagnostics_json"] if "diagnostics_json" in row.keys() else None)
        entry_price = self._optional_float(diagnostics.get("executor_entry_price")) or self._optional_float(row["entry_price"]) or 0.0
        initial_sl = (
            self._optional_float(diagnostics.get("executor_initial_sl"))
            or self._optional_float(diagnostics.get("initial_sl"))
            or self._optional_float(row["current_sl"])
            or 0.0
        )
        return SimpleNamespace(
            symbol=str(row["symbol"] or (parts[0] if parts else "")),
            side=side,
            kind=kind,
            source="executor_refresh",
            score=0.0,
            entry=entry_price,
            stop_loss=initial_sl,
            take_profit_1=None,
            take_profit_2=None,
            reasons=["open_position_refresh"],
            meta={
                "tf": timeframe,
                "market": market,
                "btc_regime": diagnostics.get("btc_regime") or "BTC_NEUTRAL",
                "market_regime": diagnostics.get("market_regime") or diagnostics.get("btc_regime") or "BTC_NEUTRAL",
            },
        )

    def _executor_snapshot_override_from_row(self, row) -> dict[str, object]:
        diagnostics = self._parse_executor_diagnostics(row["diagnostics_json"] if "diagnostics_json" in row.keys() else None)
        keys = set(row.keys())
        snapshot: dict[str, object] = {}
        for field in (
            "price",
            "spread_bps",
            "buy_flow",
            "sell_flow",
            "volume_impulse",
            "bid_wall_strength",
            "ask_wall_strength",
            "support",
            "resistance",
            "ema20",
            "vwap",
        ):
            value = self._optional_float(row[field]) if field in keys else None
            if value is None:
                value = self._optional_float(diagnostics.get(field))
            if value is not None:
                snapshot[field] = value
        snapshot.setdefault("price", self._optional_float(row["entry_price"]) or 0.0)
        snapshot.setdefault("spread_bps", 0.0)
        snapshot.setdefault("buy_flow", 1.0)
        snapshot.setdefault("sell_flow", 1.0)
        snapshot.setdefault("volume_impulse", 1.0)
        snapshot.setdefault("bid_wall_strength", 0.0)
        snapshot.setdefault("ask_wall_strength", 0.0)
        snapshot["bars_since_entry"] = int(row["bars_in_trade"] or 0) + 1
        return snapshot

    async def _executor_candle_snapshot_override(self, rest: BybitRestClient, signal) -> dict[str, object]:
        market = str(signal.meta.get("market") or "linear")
        timeframe = str(signal.meta.get("tf") or "1")
        try:
            df = await rest.fetch_klines(signal.symbol, interval=timeframe, limit=30, category=market)
        except Exception:
            return {}
        if getattr(df, "empty", True):
            return {}
        try:
            last = df.iloc[-1]
            close = self._optional_float(last.get("close"))
            if close is None or close <= 0:
                return {}
            snapshot: dict[str, object] = {"price": close, "candle_close": close}
            if "low" in df:
                snapshot["support"] = self._optional_float(df["low"].tail(20).min())
            if "high" in df:
                snapshot["resistance"] = self._optional_float(df["high"].tail(20).max())
            if "close" in df:
                snapshot["ema20"] = self._optional_float(df["close"].tail(20).ewm(span=20, adjust=False).mean().iloc[-1])
            return snapshot
        except Exception:
            return {}

    def _deferred_entry_structure_refresh_seconds(self) -> float:
        """Keep H1 structure reads bounded independently from live flow refresh."""

        return max(
            60.0,
            self._env_float(
                "EXECUTOR_DEFERRED_ENTRY_STRUCTURE_REFRESH_SECONDS",
                300.0,
            ),
        )

    async def _deferred_entry_closed_h1_structure(
        self,
        rest: BybitRestClient,
        record: dict[str, object],
    ) -> dict[str, object] | None:
        """Return structure derived exclusively from the last closed H1 candle."""

        timeframe = str(record.get("timeframe") or "").strip().lower()
        if timeframe not in {"60", "1h", "h1"}:
            return None

        symbol = str(record.get("symbol") or "").upper()
        market = str(record.get("market") or "linear").lower()

        if not symbol:
            return None

        cache = getattr(
            self,
            "_deferred_entry_structure_cache",
            None,
        )
        if not isinstance(cache, dict):
            cache = {}
            self._deferred_entry_structure_cache = cache

        cache_key = (symbol, market)
        now_monotonic = time.monotonic()
        cache_entry = cache.get(cache_key)

        if (
            isinstance(cache_entry, tuple)
            and len(cache_entry) == 2
            and isinstance(cache_entry[0], (int, float))
            and isinstance(cache_entry[1], dict)
            and now_monotonic - float(cache_entry[0])
            < self._deferred_entry_structure_refresh_seconds()
        ):
            return dict(cache_entry[1])

        try:
            raw_frame = await rest.fetch_klines(
                symbol,
                interval="60",
                limit=30,
                category=market,
            )
        except Exception:
            self.logger.debug(
                "Deferred closed-H1 structure fetch failed for %s",
                symbol,
                exc_info=True,
            )
            return None

        closed = self._closed_candle_frame(raw_frame)
        if closed is None or len(closed) < 20:
            return None

        try:
            frame = add_indicators(closed)
            last = frame.iloc[-1]

            close = self._optional_float(last.get("close"))
            support = self._optional_float(
                frame["low"].tail(20).min()
            )
            ema20 = self._optional_float(last.get("ema_20"))
        except Exception:
            self.logger.debug(
                "Deferred closed-H1 structure build failed for %s",
                symbol,
                exc_info=True,
            )
            return None

        if (
            close is None
            or close <= 0
            or support is None
            or support <= 0
            or ema20 is None
            or ema20 <= 0
        ):
            return None

        structure: dict[str, object] = {
            "price": close,
            "candle_close": close,
            "support": support,
            "ema20": ema20,
            "closed_h1_start": str(
                last.get("start", "") or ""
            ),
        }

        cache[cache_key] = (
            now_monotonic,
            dict(structure),
        )
        return structure

    async def refresh_deferred_entry_candidates(
        self,
        *,
        rest: BybitRestClient,
        stream: MarketStream,
    ) -> int:
        """Refresh deferred candidate lifecycle without opening positions.

        This path only persists PENDING / PULLBACK_SEEN / READY / terminal
        transitions. It never evaluates an entry decision and never calls
        _open_executor_position.
        """

        runtime = getattr(
            self,
            "deferred_entry_runtime",
            None,
        )
        service = getattr(
            self,
            "deferred_entry_refresh_service",
            None,
        )

        if (
            runtime is None
            or service is None
            or not runtime.config.enabled
            or self.trade_executor_mode != "paper"
        ):
            return 0

        records = runtime.coordinator.store.list_active(
            limit=service.max_active,
        )

        snapshots_by_signal_key = {}
        max_orderflow_age_seconds = max(
            5.0,
            self._env_float(
                "EXECUTOR_DEFERRED_ENTRY_MAX_ORDERFLOW_AGE_SECONDS",
                90.0,
            ),
        )

        for record in records:
            signal_key = str(record.get("signal_key") or "")
            symbol = str(record.get("symbol") or "").upper()

            if not signal_key or not symbol:
                continue

            try:
                structure = (
                    await self._deferred_entry_closed_h1_structure(
                        rest,
                        record,
                    )
                )
                if structure is None:
                    continue

                state = (
                    stream.get_state(symbol)
                    if stream is not None
                    and hasattr(stream, "get_state")
                    else None
                )

                latest_book = (
                    state.snapshots[-1]
                    if state is not None
                    and getattr(state, "snapshots", None)
                    else None
                )

                live_price = self._optional_float(
                    getattr(latest_book, "mid", None)
                )
                latest_ts = self._optional_float(
                    getattr(latest_book, "ts", None)
                )

                if (
                    latest_book is None
                    or live_price is None
                    or live_price <= 0
                    or latest_ts is None
                    or latest_ts <= 0
                    or time.time() - latest_ts
                    > max_orderflow_age_seconds
                ):
                    # No fresh orderflow means reclaim confirmation is forbidden,
                    # but closed H1 data may still advance pullback tracking,
                    # expire an old candidate, or detect stop invalidation.
                    built = build_deferred_entry_snapshot(
                        record,
                        orderflow_snapshot=None,
                        closed_h1_structure=structure,
                    )
                    if built.snapshot is not None:
                        snapshots_by_signal_key[signal_key] = (
                            built.snapshot
                        )
                    continue

                refresh_signal = SimpleNamespace(
                    symbol=symbol,
                    side=str(record.get("side") or "Buy"),
                    entry=self._optional_float(
                        record.get("origin_entry")
                    )
                    or 0.0,
                    stop_loss=self._optional_float(
                        record.get("origin_stop_loss")
                    )
                    or 0.0,
                    reasons=["deferred_entry_refresh"],
                    meta={
                        "tf": "60",
                        "market": str(
                            record.get("market") or "linear"
                        ).lower(),
                    },
                )

                live_snapshot, weak = self._paper_executor_snapshot(
                    refresh_signal,
                    state,
                )

                if weak:
                    # The live price remains useful for stop invalidation and
                    # pullback tracking, but zero flow/volume and max ask wall
                    # make READY impossible until real flow returns.
                    live_snapshot = OrderflowSnapshot(
                        price=live_price,
                        spread_bps=self._float_or_default(
                            getattr(
                                latest_book,
                                "spread_bps",
                                0.0,
                            ),
                            0.0,
                        ),
                        buy_flow=0.0,
                        sell_flow=0.0,
                        bid_wall_strength=0.0,
                        ask_wall_strength=1.0,
                        volume_impulse=0.0,
                        support=None,
                        resistance=None,
                        ema20=None,
                        vwap=None,
                        candle_close=None,
                    )
                else:
                    volume_diagnostics = (
                        self._derive_volume_impulse(
                            refresh_signal,
                            state,
                            live_snapshot.buy_flow,
                            live_snapshot.sell_flow,
                        )
                    )
                    if bool(
                        volume_diagnostics.get(
                            "volume_impulse_missing"
                        )
                    ):
                        live_snapshot = dataclasses.replace(
                            live_snapshot,
                            volume_impulse=0.0,
                        )

                built = build_deferred_entry_snapshot(
                    record,
                    orderflow_snapshot=live_snapshot,
                    closed_h1_structure=structure,
                )

                if built.snapshot is not None:
                    snapshots_by_signal_key[signal_key] = (
                        built.snapshot
                    )
            except Exception:
                self.logger.exception(
                    "Deferred entry refresh failed for %s",
                    signal_key,
                )

        batch = service.refresh_active(
            snapshots_by_signal_key,
        )

        if batch.refreshed:
            self.logger.info(
                "Deferred lifecycle refresh: refreshed=%s "
                "ready=%s terminal=%s skipped=%s",
                batch.refreshed,
                len(batch.ready_signal_keys),
                len(batch.terminal_signal_keys),
                len(batch.skipped_missing_snapshot_keys),
            )

        return batch.refreshed


    async def refresh_open_executor_positions(self, *, rest: BybitRestClient | None = None, stream: MarketStream | None = None) -> int:
        if not self.trade_executor_enabled or self.trade_executor is None:
            return 0

        open_positions = self.signal_store.list_open_executor_positions()
        refreshed = 0
        for row in open_positions:
            try:
                signal = self._executor_signal_from_outcome_row(row)
                signal.meta["executor_snapshot"] = self._executor_snapshot_override_from_row(row)

                if rest is not None:
                    signal.meta["executor_snapshot"].update(await self._executor_candle_snapshot_override(rest, signal))

                state = stream.get_state(signal.symbol) if stream is not None and hasattr(stream, "get_state") else None
                snapshot, weak = self._paper_executor_snapshot(signal, state)
                if weak:
                    snapshot, weak = self._paper_executor_snapshot(signal, None)
                if weak:
                    self.logger.debug("Skipping weak open executor position refresh for %s", row["signal_key"])
                    continue

                position = self._position_from_executor_row(signal, row)
                decision = self.trade_executor.update_position(position, snapshot)
                _no_progress_signal_key = (
                    locals().get("signal_key")
                    or locals().get("key")
                    or locals().get("trade_key")
                    or locals().get("position_key")
                    or (row["signal_key"] if "signal_key" in row.keys() else None)
                    or (row["signal_id"] if "signal_id" in row.keys() else None)
                    or (row["trade_key"] if "trade_key" in row.keys() else None)
                    or ""
                )
                decision = self._apply_no_progress_timeout_exit(
                    _no_progress_signal_key,
                    locals().get("signal"),
                    position,
                    snapshot,
                    decision,
                    row,
                )
                self._store_paper_executor_decision(str(row["signal_key"]), signal, decision, decision.position, snapshot)
                refreshed += 1
            except Exception:
                self.logger.exception("Failed to refresh open executor position %s", row["signal_key"])
        return refreshed


    def _observe_hybrid_entry_shadow(self, signal_key: str, setup: TradeSetup, snapshot: OrderflowSnapshot) -> None:
        engine = getattr(self, "hybrid_entry_shadow", None)
        if engine is None:
            executor = getattr(self, "trade_executor", None)
            engine = HybridEntryShadowEngine(
                min_volume_impulse=self._optional_float(getattr(executor, "min_entry_volume_impulse", None)) or 1.2,
                max_spread_bps=self._optional_float(getattr(executor, "max_spread_bps", None)) or 15.0,
                ask_wall_entry_limit=self._optional_float(getattr(executor, "ask_wall_entry_limit", None)) or 0.65,
            )
            self.hybrid_entry_shadow = engine
        try:
            engine.observe(store=self.signal_store, signal_key=signal_key, setup=setup, snapshot=snapshot)
        except Exception:
            self.logger.exception("Hybrid entry shadow observation failed for %s", signal_key)

    def _process_paper_executor(
        self,
        signal,
        market: str,
        confirmed_status: str | None,
        state=None,
        h4_entry_context: dict[str, object] | None = None,
    ) -> None:
        if not self.trade_executor_enabled or self.trade_executor is None:
            return
        should_process, observation_context = self._should_process_paper_executor_status(signal, confirmed_status)
        signal_key = self._signal_key(signal, market)
        existing = self.signal_store.get_executor_outcome(signal_key)
        snapshot, weak = self._paper_executor_snapshot(signal, state)
        setup = self._paper_executor_setup(signal)
        observation_context = dict(observation_context or {})
        observation_context.update(h4_entry_context or {})
        observation_context.update(self._missed_signal_memory_diagnostics(setup))

        deferred_probe_only = False
        deferred_runtime = getattr(
            self,
            "deferred_entry_runtime",
            None,
        )

        if (
            not should_process
            and deferred_runtime is not None
        ):
            try:
                deferred_probe = (
                    deferred_runtime.probe_early_signal(
                        mode=self.trade_executor_mode,
                        timeframe=str(setup.timeframe),
                        side=str(setup.side),
                        signal_kind=str(setup.signal_kind),
                        confirmed_status=confirmed_status,
                        score=float(setup.score),
                        btc_regime=str(setup.btc_regime),
                    )
                )
                observation_context.update(
                    {
                        "deferred_entry_probe_only": bool(
                            deferred_probe.allowed
                        ),
                        "deferred_entry_probe_reason": (
                            deferred_probe.reason
                        ),
                    }
                )

                if deferred_probe.allowed:
                    should_process = True
                    deferred_probe_only = True
            except Exception:
                self.logger.exception(
                    "Deferred-entry probe policy failed "
                    "for %s",
                    signal_key,
                )
                observation_context.update(
                    {
                        "deferred_entry_probe_only": False,
                        "deferred_entry_probe_reason": (
                            "deferred_entry_probe_error"
                        ),
                    }
                )

        forced_early_entry = False
        forced_early_decision = None
        if not should_process and not weak:
            forced_early_decision, early_context = self._evaluate_early_breakout_entry(
                signal_key,
                signal,
                setup,
                snapshot,
            )
            observation_context.update(early_context)
            if forced_early_decision is not None:
                should_process = True
                forced_early_entry = True
        if not should_process:
            self._record_signal_forward_outcome(
                signal_key,
                signal,
                status=confirmed_status,
                snapshot=None if weak else snapshot,
                executor_block_reason="executor_not_processed_signal_status",
            )
            return

        self._observe_hybrid_entry_shadow(signal_key, setup, snapshot)

        existing_is_terminal = self._is_terminal_executor_outcome(existing)

        if weak:
            current_state = "TRADE_WATCH" if existing_is_terminal or existing is None else str(existing["state"])
            decision = TradeDecision(WATCH, "paper_executor_missing_snapshot_data", current_state, None)
            self._store_paper_executor_decision(signal_key, signal, decision, None, snapshot, setup=setup, observation_context=observation_context)
            return

        if existing is not None and not existing_is_terminal and str(existing["state"]) in self.ACTIVE_EXECUTOR_OUTCOME_STATES:
            position = self._position_from_executor_row(signal, existing)
            if not self._executor_side_allowed(str(position.side)):
                exit_position = dataclasses.replace(
                    position,
                    state="EXITED",
                    exit_price=float(snapshot.price),
                    exit_reason="exit_executor_side_disabled",
                )
                decision = TradeDecision(
                    EXIT,
                    "exit_executor_side_disabled",
                    "EXITED",
                    exit_position,
                )
                self._store_paper_executor_decision(signal_key, signal, decision, decision.position, snapshot, setup=setup, observation_context=observation_context)
                return
            decision = self.trade_executor.update_position(position, snapshot)
            self._store_paper_executor_decision(signal_key, signal, decision, decision.position, snapshot, setup=setup, observation_context=observation_context)
            return

        entry_decision = (
            self.trade_executor.evaluate_entry(
                setup,
                snapshot,
            )
            if deferred_probe_only
            else (
                forced_early_decision
                or self.trade_executor.evaluate_entry(
                    setup,
                    snapshot,
                )
            )
        )

        if not deferred_probe_only:
            entry_decision = (
                self._executor_buy_momentum_override_decision(
                    setup,
                    snapshot,
                    entry_decision,
                )
            )
        if not self._executor_side_allowed(str(setup.side)):
            entry_decision = TradeDecision(
                "WATCH",
                "entry_blocked_executor_side_not_allowed",
                "TRADE_WATCH",
                None,
            )

        if (
            not deferred_probe_only
            and entry_decision.action
            not in {ENTER_LONG, ENTER_SHORT}
        ):
            reentry_decision, reentry_context = self._evaluate_stop_reclaim_reentry(signal_key, signal, setup, snapshot)
            observation_context.update(reentry_context)
            if reentry_decision is not None:
                entry_decision = reentry_decision
                forced_early_entry = True
        if (
            not deferred_probe_only
            and entry_decision.action
            not in {ENTER_LONG, ENTER_SHORT}
        ):
            early_decision, early_context = self._evaluate_early_breakout_entry(signal_key, signal, setup, snapshot)
            observation_context.update(early_context)
            if early_decision is not None:
                entry_decision = early_decision
                forced_early_entry = True
        if (
            deferred_probe_only
            and entry_decision.action
            in {ENTER_LONG, ENTER_SHORT}
        ):
            observation_context.update(
                {
                    "deferred_entry_registered": False,
                    "deferred_entry_registration_reason": (
                        "deferred_entry_probe_entry_already_allowed"
                    ),
                    "deferred_entry_probe_result": (
                        "entry_allowed_no_immediate_entry"
                    ),
                }
            )
            entry_decision = TradeDecision(
                WATCH,
                "deferred_entry_probe_entry_already_allowed",
                "TRADE_WATCH",
                None,
            )

        # Final side gate: reentry/early overrides must not bypass EXECUTOR_ALLOWED_SIDES.
        if entry_decision.action in {ENTER_LONG, ENTER_SHORT} and not self._executor_side_allowed(str(setup.side)):
            entry_decision = TradeDecision(
                WATCH,
                "entry_blocked_executor_side_not_allowed",
                "TRADE_WATCH",
                None,
            )
            forced_early_entry = False

        if entry_decision.action in {ENTER_LONG, ENTER_SHORT}:
            if not bool(observation_context.get("h4_entry_gate_allowed", True)):
                h4_gate_reason = str(
                    observation_context.get("h4_entry_gate_reason")
                    or "entry_blocked_h4_bearish_structure"
                )
                h4_gate_decision = TradeDecision(
                    WATCH,
                    h4_gate_reason,
                    "TRADE_WATCH",
                    None,
                )
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    h4_gate_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=observation_context,
                )
                return

            if self._executor_symbol_blocked(str(signal.symbol)):
                block_decision = TradeDecision(WATCH, "entry_blocked_symbol_blocklist", "TRADE_WATCH", None)
                block_context = {
                    "executor_symbol_blocklist": os.getenv("EXECUTOR_SYMBOL_BLOCKLIST", ""),
                    "executor_symbol_blocked": True,
                }
                observation_context.update(block_context)
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    block_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=observation_context,
                )
                return

            target_quality_decision, target_quality_context = self._executor_target_quality_gate(
                signal,
                setup,
                snapshot,
                confirmed_status,
            )
            observation_context.update(target_quality_context)
            if target_quality_decision is not None:
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    target_quality_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=observation_context,
                )
                return

            rr_guard_decision, rr_guard_context = self._entry_risk_reward_guard(signal, setup, snapshot)
            observation_context.update(rr_guard_context)
            if rr_guard_decision is not None:
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    rr_guard_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=observation_context,
                )
                return

            symbol_lock_decision, symbol_lock_context = self._executor_symbol_position_lock(signal_key, setup)
            observation_context.update(symbol_lock_context)
            if symbol_lock_decision is not None:
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    symbol_lock_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=observation_context,
                )
                return

            late_chase_watch_decision, late_chase_context = self._evaluate_late_chase_gate(
                signal_key,
                signal,
                setup,
                snapshot,
            )
            observation_context.update(late_chase_context)
            if late_chase_watch_decision is not None:
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    late_chase_watch_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=observation_context,
                )
                return
            learning_watch_decision, learning_context = self._evaluate_executor_learning_gate(setup)
            entry_observation_context = dict(observation_context or {})
            entry_observation_context.update(learning_context)
            if learning_watch_decision is not None:
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    learning_watch_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=entry_observation_context,
                )
                return
            stop_guard_decision, stop_guard_context = self._entry_stop_loss_guard(setup, snapshot)
            entry_observation_context.update(stop_guard_context)
            if stop_guard_decision is not None:
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    stop_guard_decision,
                    None,
                    snapshot,
                    setup=setup,
                    observation_context=entry_observation_context,
                )
                return

            if self.trade_executor_mode == "testnet":
                testnet_result = self._execute_testnet_entry(signal_key, signal, snapshot)
                if not testnet_result.get("ok"):
                    watch_decision = TradeDecision(WATCH, str(testnet_result.get("reason") or "entry_blocked_testnet"), "TRADE_WATCH", None)
                    self._store_paper_executor_decision(
                        signal_key,
                        signal,
                        watch_decision,
                        None,
                        snapshot,
                        setup=setup,
                        testnet_result=testnet_result,
                        observation_context=entry_observation_context,
                    )
                    return
                position = self._open_executor_position(setup, snapshot, force=forced_early_entry)
                entry_decision = TradeDecision(entry_decision.action, entry_decision.reason, ENTERED, position)
                self._store_paper_executor_decision(
                    signal_key,
                    signal,
                    entry_decision,
                    position,
                    snapshot,
                    setup=setup,
                    testnet_result=testnet_result,
                    observation_context=entry_observation_context,
                )
                return
            position = self._open_executor_position(setup, snapshot, force=forced_early_entry)
            entry_decision = TradeDecision(entry_decision.action, entry_decision.reason, ENTERED, position)
            self._store_paper_executor_decision(
                signal_key,
                signal,
                entry_decision,
                position,
                snapshot,
                setup=setup,
                observation_context=entry_observation_context,
            )
            return

        deferred_runtime = getattr(
            self,
            "deferred_entry_runtime",
            None,
        )

        if (
            deferred_runtime is not None
            and deferred_runtime.config.enabled
            and str(entry_decision.reason)
            != "deferred_entry_probe_entry_already_allowed"
        ):
            long_blockers = (
                self.trade_executor._long_entry_blockers(
                    setup,
                    snapshot,
                )
                if str(setup.side) == "Buy"
                else ["entry_blocked_not_buy_side"]
            )

            structural_blockers = [
                blocker
                for blocker in long_blockers
                if blocker not in TRANSIENT_ENTRY_BLOCK_REASONS
            ]

            target_guard, _ = self._executor_target_quality_gate(
                signal,
                setup,
                snapshot,
                confirmed_status,
            )
            rr_guard, _ = self._entry_risk_reward_guard(
                signal,
                setup,
                snapshot,
            )
            stop_guard, _ = self._entry_stop_loss_guard(
                setup,
                snapshot,
            )

            if target_guard is not None:
                structural_blockers.append(
                    str(target_guard.reason)
                )

            if rr_guard is not None:
                structural_blockers.append(
                    str(rr_guard.reason)
                )

            if stop_guard is not None:
                structural_blockers.append(
                    str(stop_guard.reason)
                )

            if self._executor_symbol_blocked(
                str(signal.symbol)
            ):
                structural_blockers.append(
                    "entry_blocked_symbol_blocklist"
                )

            observation_context.update(
                register_deferred_watch(
                    runtime=deferred_runtime,
                    mode=self.trade_executor_mode,
                    signal_key=signal_key,
                    signal=signal,
                    setup=setup,
                    snapshot=snapshot,
                    market=market,
                    block_reason=str(entry_decision.reason),
                    confirmed_status=confirmed_status,
                    h4_allowed=bool(
                        observation_context.get(
                            "h4_entry_gate_allowed",
                            False,
                        )
                    ),
                    structural_allowed=not structural_blockers,
                    structural_blockers=structural_blockers,
                )
            )

        watch_decision = TradeDecision(
            WATCH,
            entry_decision.reason,
            "TRADE_WATCH",
            None,
        )
        self._store_paper_executor_decision(
            signal_key,
            signal,
            watch_decision,
            None,
            snapshot,
            setup=setup,
            observation_context=observation_context,
        )

    async def _emit_signal(self, rest: BybitRestClient, signal, state=None) -> None:
        market = str(
            signal.meta.get(
                "market",
                self.settings.market_categories[0].lower() if self.settings.market_categories else "linear",
            )
        )

        upsert = self.signal_store.upsert_signal(signal, market=market)
        promoted, promoted_to, promoted_reasons = self._maybe_promote_confirmed(signal, upsert, market)
        confirmed_status = promoted_to or upsert.to_status

        signal_key = self._signal_key(signal, market)
        self._record_signal_lifecycle(signal, signal_key, upsert, confirmed_status)

        h4_entry_context = None
        if self.trade_executor_enabled:
            h4_entry_context = await self._h4_long_entry_gate_context(rest, signal)

        self._process_paper_executor(
            signal,
            market,
            confirmed_status,
            state,
            h4_entry_context=h4_entry_context,
        )

        now = time.time()
        cooldown = self._cooldown_seconds(signal)
        cooldown_key = f"{signal.dedupe_key()}|{signal.meta.get('tf', 'na')}"
        last_sent = self._cooldowns.get(cooldown_key, 0.0)

        if now - last_sent < cooldown:
            return

        self._cooldowns[cooldown_key] = now
        self._counts[signal.source] += 1

        log_body = (
            f"{signal.kind}\n"
            f"#{signal.symbol} | {signal.side} | score={signal.score}\n"
            f"entry={signal.entry:.8f}\n"
            f"sl={signal.stop_loss:.8f}\n"
            f"tp1={signal.take_profit_1:.8f}\n"
            f"tp2={signal.take_profit_2:.8f}\n"
            f"reasons: {', '.join(signal.reasons)}\n"
            f"meta: {', '.join(f'{k}={v}' for k, v in signal.meta.items())}"
        )

        target_logger = self.orderflow_logger if signal.source == "orderflow" else self.macro_logger
        target_logger.info("📡 %s", log_body)

        if not upsert.should_notify and not promoted:
            return

        # Policy: CSV/UI are notify-only to avoid repeat-noise pollution.
        self.csv_logger.append(signal)
        self.ui.update_session(
            macro=self._counts["macro"],
            orderflow=self._counts["orderflow"],
        )
        self.ui.print_signal(signal)

        await self.dashboard.post_signal(signal)

        if promoted:
            await self.dashboard.post_log(
                f"{signal.symbol} {signal.meta.get('tf', 'na')}: promoted to {promoted_to} ({', '.join(promoted_reasons)})",
                source="confirmed_promoter",
                severity="success",
            )

        if upsert.status_changed:
            await self.dashboard.post_log(
                f"{signal.symbol} {signal.meta.get('tf', 'na')}: stage {upsert.from_status or 'NEW'} -> {upsert.to_status}",
                source="signal_store",
                severity="success",
            )
        elif upsert.score_jump:
            await self.dashboard.post_log(
                f"{signal.symbol} {signal.meta.get('tf', 'na')}: score jump detected ({signal.score:.2f})",
                source="signal_store",
                severity="info",
            )

        chart_path = await self._build_chart_for_signal(rest, signal)

        try:
            await self.telegram.send_signal(
                signal.symbol,
                signal.side,
                signal.entry,
                signal.stop_loss,
                signal.take_profit_1,
                signal.take_profit_2,
                signal.reasons,
                photo_path=chart_path,
                title=signal.kind,
                timeframe=str(signal.meta.get("tf", "")),
            )
        except Exception as exc:
            self.logger.warning("Signal notify failed for %s: %r", signal.symbol, exc)
