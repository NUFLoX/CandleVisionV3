from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
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
from .confirmed_promoter import ConfirmedPromoter
from .indicators import add_indicators
from .executor_exit_shadow import (
    DEFAULT_EXIT_SHADOW_POLICY,
    current_unrealized_r,
    evaluate_exit_shadow_policy,
    utc_now_iso,
)
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
        self.executor_exit_shadow_enabled = os.getenv("EXECUTOR_EXIT_SHADOW_ENABLED", "false").strip().lower() == "true"
        self.executor_exit_shadow_policy = os.getenv("EXECUTOR_EXIT_SHADOW_POLICY", DEFAULT_EXIT_SHADOW_POLICY).strip() or DEFAULT_EXIT_SHADOW_POLICY
        self.trade_learning = TradeLearningEngine(self.signal_store, logger=self.logger)
        self.dashboard = DashboardIngestClient()
        self.promoter = ConfirmedPromoter()
        self._cooldowns: dict[str, float] = {}
        self._counts = {"macro": 0, "orderflow": 0}
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

    def _build_trade_executor(self) -> SmartTradeExecutor:
        if self.trade_executor_mode != "paper":
            return SmartTradeExecutor(management_policy=MANAGEMENT_POLICY_LEGACY, trade_executor_mode=self.trade_executor_mode)
        return SmartTradeExecutor(
            trade_executor_mode=self.trade_executor_mode,
            management_policy=os.getenv("EXECUTOR_MANAGEMENT_POLICY", MANAGEMENT_POLICY_LEGACY),
            protect_after_1r=self._env_bool("EXECUTOR_PROTECT_AFTER_1R", False),
            min_protected_r_after_1r=self._env_float("EXECUTOR_MIN_PROTECTED_R_AFTER_1R", 0.25),
        )

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

    async def run(self) -> None:
        async with BybitRestClient(
            self.settings.rest_base_url,
            timeout_seconds=self.settings.rest_timeout_seconds,
            retries=self.settings.rest_retries,
        ) as rest:
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
                f"Signal mode: {self.settings.signal_mode}"
            )

            tasks = [
                asyncio.create_task(stream.run([target.symbol for target in realtime_symbols]), name="accum_ws"),
                asyncio.create_task(self._run_realtime_scan(rest, stream, realtime_symbols), name="accum_realtime"),
                asyncio.create_task(self._run_macro_scan(rest, macro_symbols), name="accum_macro"),
                asyncio.create_task(self._run_status(stream, len(realtime_symbols), len(macro_symbols)), name="accum_status"),
            ]
            if self.trade_executor_enabled:
                tasks.append(
                    asyncio.create_task(self._run_executor_maintenance(rest, stream), name="accum_executor_maintenance")
                )

            await asyncio.gather(*tasks)

    async def _run_status(self, stream: MarketStream, realtime_count: int, macro_count: int) -> None:
        while True:
            await self.dashboard.post_heartbeat(
                "scanner",
                meta={
                    "runner": "orderflow_accum",
                    "loop": "status",
                    "ws_status": stream.status,
                    "realtime_symbols": realtime_count,
                    "macro_symbols": macro_count,
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
                refreshed = await self.refresh_open_executor_positions(rest=rest, stream=stream)
            except Exception:
                self.logger.exception("Open executor position refresh failed")
            await self._post_executor_heartbeat(loop="executor_maintenance", refreshed=refreshed)
            await asyncio.sleep(refresh_seconds)

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
            await self.dashboard.post_heartbeat(
                "scanner",
                meta={
                    "runner": "orderflow_accum",
                    "loop": "realtime",
                    "symbols": len(symbols),
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
            await self.dashboard.post_heartbeat(
                "scanner",
                meta={
                    "runner": "orderflow_accum",
                    "loop": "macro",
                    "symbols": len(symbols),
                },
            )

            for target in symbols:
                try:
                    symbol = target.symbol
                    frames = {}

                    for interval, limit in intervals.items():
                        frames[interval] = await rest.fetch_klines(
                            symbol,
                            interval=interval,
                            limit=limit,
                            category=target.market,
                        )
                        await asyncio.sleep(0.04)

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
    def _normalize_active_r_scale(
        cls,
        *,
        side: str,
        entry_price: float,
        initial_risk: float | None,
        max_gain_r: float,
        max_drawdown_r: float,
        diagnostics_json: dict[str, object],
    ) -> tuple[float, float, dict[str, object]]:
        if initial_risk is None or initial_risk <= 0:
            return max_gain_r, max_drawdown_r, {}
        suspicious_gain = abs(float(max_gain_r)) > cls.ACTIVE_R_SUSPICIOUS_THRESHOLD
        suspicious_drawdown = abs(float(max_drawdown_r)) > cls.ACTIVE_R_SUSPICIOUS_THRESHOLD
        if not suspicious_gain and not suspicious_drawdown:
            return max_gain_r, max_drawdown_r, {"suspicious_active_r_scale": False}

        updates: dict[str, object] = {
            "suspicious_active_r_scale": True,
            "active_r_scale_original_max_gain_r": float(max_gain_r),
            "active_r_scale_original_max_drawdown_r": float(max_drawdown_r),
        }
        max_price, min_price = cls._active_price_extremes_from_diagnostics(diagnostics_json)
        if max_price is not None and min_price is not None:
            recomputed_gain, recomputed_drawdown = SmartTradeExecutor.price_distance_r_metrics(
                side=side,
                entry_price=float(entry_price),
                initial_risk=float(initial_risk),
                max_price=float(max_price),
                min_price=float(min_price),
            )
            updates.update(
                {
                    "active_r_recomputed_from_price_extremes": True,
                    "suspicious_active_r_scale": False,
                    "active_r_scale_fix": "price_extremes",
                }
            )
            return recomputed_gain, recomputed_drawdown, updates

        gain_pct = cls._optional_float(diagnostics_json.get("max_gain_pct"))
        if gain_pct is None:
            gain_pct = cls._optional_float(diagnostics_json.get("max_gain_percent"))
        drawdown_pct = cls._optional_float(diagnostics_json.get("max_drawdown_pct"))
        if drawdown_pct is None:
            drawdown_pct = cls._optional_float(diagnostics_json.get("max_drawdown_percent"))
        if gain_pct is not None or drawdown_pct is not None:
            recomputed_gain = (
                cls._active_r_from_fractional_price_move(
                    entry_price=float(entry_price), initial_risk=float(initial_risk), move=float(gain_pct)
                )
                if gain_pct is not None
                else float(max_gain_r)
            )
            recomputed_drawdown = (
                cls._active_r_from_fractional_price_move(
                    entry_price=float(entry_price), initial_risk=float(initial_risk), move=float(drawdown_pct)
                )
                if drawdown_pct is not None
                else float(max_drawdown_r)
            )
            updates.update(
                {
                    "active_r_recomputed_from_percent_move": True,
                    "active_r_scale_fix": "percent_move_normalized",
                }
            )
            return recomputed_gain, recomputed_drawdown, updates

        updates.update(
            {
                "active_r_price_extremes_missing": True,
                "active_r_scale_fix": "legacy_x100_r_normalized",
            }
        )
        normalized_gain = float(max_gain_r) / 100.0 if suspicious_gain else float(max_gain_r)
        normalized_drawdown = float(max_drawdown_r) / 100.0 if suspicious_drawdown else float(max_drawdown_r)
        return normalized_gain, normalized_drawdown, updates

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
        max_gain_r = 0.0 if invalid_initial_risk else float(row["max_gain_r"] or 0.0)
        max_drawdown_r = 0.0 if invalid_initial_risk else float(row["max_drawdown_r"] or 0.0)
        active_r_diagnostics: dict[str, object] = {}
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
            ("current_sl", row["current_sl"]),
        ]
        for source, value in candidates:
            parsed = self._optional_float(value)
            if parsed is None:
                continue
            risk_diagnostics: dict[str, object] = {"risk_basis": "initial_sl", "risk_source": source}
            if source == "current_sl":
                risk_diagnostics["risk_basis_warning"] = "fallback_current_sl_missing_initial_sl"
            return parsed, risk_diagnostics
        fallback = self._optional_float(getattr(signal, "stop_loss", None)) or self._optional_float(row["current_sl"]) or entry_price
        return float(fallback), {
            "risk_basis": "initial_sl",
            "risk_source": "fallback_signal_stop_loss",
            "risk_basis_warning": "fallback_current_sl_missing_initial_sl" if self._optional_float(row["current_sl"]) is not None else "missing_initial_sl",
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

    def _store_paper_executor_decision(self, signal_key: str, signal, decision, position=None, snapshot=None, setup=None, testnet_result=None):
        previous_row = self.signal_store.get_executor_outcome(signal_key)
        diagnostics = self._paper_executor_diagnostics(signal, snapshot)
        diagnostics_json = diagnostics.get("diagnostics_json")
        if not isinstance(diagnostics_json, dict):
            diagnostics_json = self._parse_executor_diagnostics(diagnostics_json)
            diagnostics["diagnostics_json"] = diagnostics_json
        is_new_entry = position is not None and str(decision.action) in {ENTER_LONG, ENTER_SHORT}
        self._preserve_executor_entry_diagnostics(diagnostics_json, previous_row, preserve_breakeven_time=not is_new_entry)
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
            max_gain_r, max_drawdown_r, active_r_diagnostics = self._normalize_active_r_scale(
                side=str(position.side),
                entry_price=float(position.entry_price),
                initial_risk=float(position.initial_risk),
                max_gain_r=max_gain_r,
                max_drawdown_r=max_drawdown_r,
                diagnostics_json=diagnostics_json,
            )
            diagnostics_json.update(active_r_diagnostics)
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
            moved_to_breakeven = bool(
                (position is not None and str(getattr(position, "state", "")) in {PROTECT_BREAKEVEN, TRAILING_PROFIT})
                or (previous_row is not None and str(previous_row["state"]) in {PROTECT_BREAKEVEN, TRAILING_PROFIT})
                or (previous_row is not None and str(previous_row["action"]) == MOVE_SL_TO_BREAKEVEN)
            )
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

            self.signal_store.upsert_executor_trade(
                {
                    "trade_key": self._stable_executor_trade_key(signal_key, entry_time, exit_time, exit_price, exit_reason),
                    "signal_key": signal_key,
                    "symbol": str(signal.symbol),
                    "timeframe": str(diagnostics_payload.get("executor_timeframe") or signal.meta.get("tf") or "1"),
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
                    "max_gain_r": self._optional_float(row["max_gain_r"]),
                    "max_drawdown_r": self._optional_float(row["max_drawdown_r"]),
                    "bars_in_trade": int(row["bars_in_trade"] or 0),
                    "duration_minutes": self._duration_minutes(entry_time, exit_time),
                    "moved_to_breakeven": moved_to_breakeven,
                    "breakeven_time": breakeven_time,
                    "diagnostics_json": diagnostics_payload,
                    "created_at": entry_time,
                    "updated_at": exit_time,
                }
            )
        except Exception:
            self.logger.exception("Failed to write executor_trades row for %s", signal_key)

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
                self._store_paper_executor_decision(str(row["signal_key"]), signal, decision, decision.position, snapshot)
                refreshed += 1
            except Exception:
                self.logger.exception("Failed to refresh open executor position %s", row["signal_key"])
        return refreshed

    def _process_paper_executor(self, signal, market: str, confirmed_status: str | None, state=None) -> None:
        if not self.trade_executor_enabled or self.trade_executor is None:
            return
        if confirmed_status not in {"CONFIRMED_LONG", "CONFIRMED_SHORT"}:
            return

        signal_key = self._signal_key(signal, market)
        existing = self.signal_store.get_executor_outcome(signal_key)
        snapshot, weak = self._paper_executor_snapshot(signal, state)
        setup = self._paper_executor_setup(signal)

        if weak:
            current_state = str(existing["state"]) if existing is not None else "TRADE_WATCH"
            decision = TradeDecision(WATCH, "paper_executor_missing_snapshot_data", current_state, None)
            self._store_paper_executor_decision(signal_key, signal, decision, None, snapshot, setup=setup)
            return

        if existing is not None and str(existing["state"]) in {ENTERED, PROTECT_BREAKEVEN, TRAILING_PROFIT}:
            position = self._position_from_executor_row(signal, existing)
            decision = self.trade_executor.update_position(position, snapshot)
            self._store_paper_executor_decision(signal_key, signal, decision, decision.position, snapshot, setup=setup)
            return

        entry_decision = self.trade_executor.evaluate_entry(setup, snapshot)
        if entry_decision.action in {ENTER_LONG, ENTER_SHORT}:
            if self.trade_executor_mode == "testnet":
                testnet_result = self._execute_testnet_entry(signal_key, signal, snapshot)
                if not testnet_result.get("ok"):
                    watch_decision = TradeDecision(WATCH, str(testnet_result.get("reason") or "entry_blocked_testnet"), "TRADE_WATCH", None)
                    self._store_paper_executor_decision(
                        signal_key, signal, watch_decision, None, snapshot, setup=setup, testnet_result=testnet_result
                    )
                    return
                position = self.trade_executor.open_position(setup, snapshot)
                entry_decision = TradeDecision(entry_decision.action, entry_decision.reason, ENTERED, position)
                self._store_paper_executor_decision(
                    signal_key, signal, entry_decision, position, snapshot, setup=setup, testnet_result=testnet_result
                )
                return
            position = self.trade_executor.open_position(setup, snapshot)
            entry_decision = TradeDecision(entry_decision.action, entry_decision.reason, ENTERED, position)
            self._store_paper_executor_decision(signal_key, signal, entry_decision, position, snapshot, setup=setup)
            return

        watch_decision = TradeDecision(WATCH, entry_decision.reason, "TRADE_WATCH", None)
        self._store_paper_executor_decision(signal_key, signal, watch_decision, None, snapshot, setup=setup)

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

        self._process_paper_executor(signal, market, confirmed_status, state)

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
