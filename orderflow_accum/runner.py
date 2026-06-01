from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
import os
import time
from datetime import UTC, datetime

from dashboard.ingest_client import DashboardIngestClient

from .bybit_rest import BybitRestClient, ScanTarget
from .config import Settings
from .console_ui import ConsoleUI
from .engines import MacroAccumulationEngine, RealtimeAccumulationEngine
from .short_engine import DistributionShortEngine
from .market_regime import MarketRegimeAnalyzer
from .chart_render import render_signal_chart
from .signal_logger import RejectionCsvLogger, SignalCsvLogger
from .signal_store import SignalStore
from .confirmed_promoter import ConfirmedPromoter
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
    OrderflowSnapshot,
    SmartTradeExecutor,
    TradeDecision,
    TradePosition,
    TradeSetup,
)
from .ws_clients import MarketStream


class AccumulationRunner:
    VOLUME_IMPULSE_REPORT_CAP = 50.0

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
        self.trade_executor_mode = os.getenv("TRADE_EXECUTOR_MODE", "paper").strip().lower()
        self.trade_executor_enabled = (
            os.getenv("RUN_TRADE_EXECUTOR", "false").strip().lower() == "true"
            and self.trade_executor_mode == "paper"
        )
        self.trade_executor = SmartTradeExecutor() if self.trade_executor_enabled else None
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

            await asyncio.sleep(30)

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

            btc_frames = {}

            try:
                for tf in self.settings.btc_regime_intervals:
                    btc_frames[tf] = await rest.fetch_klines(
                        "BTCUSDT",
                        interval=tf,
                        limit=120,
                        category="linear",
                    )
            except Exception:
                btc_frames = {}

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
                            signal.meta["btc_regime"] = regime.btc_regime

                        short_signals = []

                        if self.settings.enable_short_engine and target.market == "linear":
                            short_signals = self.short_engine.analyze(symbol, df, state, regime)

                            for signal in short_signals:
                                signal.meta["btc_regime"] = regime.btc_regime

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

    def _position_from_executor_row(self, signal, row) -> TradePosition:
        side = str(row["side"] or signal.side)
        entry_snapshot = self._executor_entry_snapshot_from_row(row)
        entry = float(entry_snapshot.get("executor_entry_price") or row["entry_price"] or signal.entry or 0.0)
        stop = float(entry_snapshot.get("executor_initial_sl") or signal.stop_loss or row["current_sl"] or 0.0)
        initial_risk = abs(entry - stop) or max(abs(entry) * 0.01, 1e-9)
        max_gain_r = float(row["max_gain_r"] or 0.0)
        max_drawdown_r = float(row["max_drawdown_r"] or 0.0)
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

        diagnostics_json = {
            **thresholds,
            "volume_impulse_source": volume_diagnostics.get("volume_impulse_source"),
            "volume_impulse_missing": bool(volume_diagnostics.get("volume_impulse_missing", False)),
            "volume_impulse_raw": volume_diagnostics.get("volume_impulse_raw"),
            "volume_baseline": self._optional_float(volume_diagnostics.get("volume_baseline")),
            "volume_current": self._optional_float(volume_diagnostics.get("volume_current")),
            "volume_impulse_ratio_to_required": volume_ratio_to_required,
            **self._volume_impulse_report_cap_fields(volume_impulse, required_volume),
        }
        values["diagnostics_json"] = diagnostics_json
        return values

    def _store_paper_executor_decision(self, signal_key: str, signal, decision, position=None, snapshot=None):
        previous_row = self.signal_store.get_executor_outcome(signal_key)
        diagnostics = self._paper_executor_diagnostics(signal, snapshot)
        diagnostics_json = diagnostics.get("diagnostics_json")
        if not isinstance(diagnostics_json, dict):
            diagnostics_json = self._parse_executor_diagnostics(diagnostics_json)
            diagnostics["diagnostics_json"] = diagnostics_json
        previous_entry_snapshot = self._executor_entry_snapshot_from_row(previous_row)
        if previous_entry_snapshot:
            diagnostics_json.update(previous_entry_snapshot)
        if position is not None and str(decision.action) in {ENTER_LONG, ENTER_SHORT}:
            diagnostics_json.update(
                {
                    "executor_entry_time": datetime.now(UTC).isoformat(),
                    "executor_entry_price": float(position.entry_price),
                    "executor_initial_sl": float(position.stop_loss),
                    "executor_side": str(position.side),
                    "executor_signal_key": signal_key,
                    "executor_timeframe": str(signal.meta.get("tf") or "1"),
                }
            )
        if (
            str(decision.reason) == "entry_blocked_volume_impulse"
            and diagnostics_json.get("volume_impulse_source") == "missing_default"
        ):
            diagnostics_json["blocker_root_cause"] = "missing_volume_impulse_mapping"
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
            max_gain_r=float(position.max_gain_r) if position is not None else 0.0,
            max_drawdown_r=float(position.max_drawdown_r) if position is not None else 0.0,
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
        return {
            key: diagnostics.get(key)
            for key in (
                "executor_entry_time",
                "executor_entry_price",
                "executor_initial_sl",
                "executor_side",
                "executor_signal_key",
                "executor_timeframe",
            )
            if diagnostics.get(key) not in (None, "")
        }

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
            diagnostics_payload.update(self._executor_entry_snapshot_from_row(row))
            diagnostics_payload.update(self._executor_entry_snapshot_from_row(previous_row))
            entry_time = diagnostics_payload.get("executor_entry_time") or (
                previous_row["created_at"] if previous_row is not None else row["created_at"]
            )
            exit_time = row["updated_at"]
            initial_sl = self._optional_float(diagnostics_payload.get("executor_initial_sl"))
            if initial_sl is None:
                initial_sl = self._optional_float(position.stop_loss) if position is not None else self._optional_float(signal.stop_loss)
            current_sl = self._optional_float(row["current_sl"])
            exit_price = self._optional_float(row["exit_price"])
            entry_price = self._optional_float(diagnostics_payload.get("executor_entry_price"))
            if entry_price is None:
                entry_price = self._optional_float(row["entry_price"])
            side = str(diagnostics_payload.get("executor_side") or row["side"])
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
                breakeven_time = breakeven_event.get("created_at") if breakeven_event else None
                if breakeven_time is None and previous_row is not None and str(previous_row["action"]) == MOVE_SL_TO_BREAKEVEN:
                    breakeven_time = previous_row["updated_at"]

            entry_action = ENTER_SHORT if str(row["side"]) == "Sell" else ENTER_LONG
            if previous_row is not None and str(previous_row["action"]) in {ENTER_LONG, ENTER_SHORT}:
                entry_action = str(previous_row["action"])

            self.signal_store.upsert_executor_trade(
                {
                    "trade_key": self._stable_executor_trade_key(signal_key, entry_time, exit_time, exit_price, row["exit_reason"]),
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
                    "exit_reason": row["exit_reason"] or decision.reason,
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
            self._store_paper_executor_decision(signal_key, signal, decision, None, snapshot)
            return

        if existing is not None and str(existing["state"]) in {ENTERED, PROTECT_BREAKEVEN, TRAILING_PROFIT}:
            position = self._position_from_executor_row(signal, existing)
            decision = self.trade_executor.update_position(position, snapshot)
            self._store_paper_executor_decision(signal_key, signal, decision, decision.position, snapshot)
            return

        entry_decision = self.trade_executor.evaluate_entry(setup, snapshot)
        if entry_decision.action in {ENTER_LONG, ENTER_SHORT}:
            position = self.trade_executor.open_position(setup, snapshot)
            entry_decision = TradeDecision(entry_decision.action, entry_decision.reason, ENTERED, position)
            self._store_paper_executor_decision(signal_key, signal, entry_decision, position, snapshot)
            return

        watch_decision = TradeDecision(WATCH, entry_decision.reason, "TRADE_WATCH", None)
        self._store_paper_executor_decision(signal_key, signal, watch_decision, None, snapshot)

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
