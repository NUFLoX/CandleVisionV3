
from __future__ import annotations

import asyncio
import fnmatch
import logging
import time

from dashboard.ingest_client import DashboardIngestClient

from .bybit_rest import BybitRestClient, ScanTarget
from .config import Settings
from .console_ui import ConsoleUI
from .engines import MacroAccumulationEngine, RealtimeAccumulationEngine
from .chart_render import render_signal_chart
from .signal_logger import RejectionCsvLogger, SignalCsvLogger
from .signal_store import SignalStore
from .telegram_notify import TelegramNotifier
from .ws_clients import MarketStream


class AccumulationRunner:
    def __init__(self, settings: Settings, ui: ConsoleUI | None = None, version: str = "ACCUM V1.4.2 DIAG"):
        self.settings = settings
        self.ui = ui or ConsoleUI()
        self.version = version
        self.logger = logging.getLogger("Accum.Runner")
        self.macro_logger = logging.getLogger("Accum.Signal.Macro")
        self.orderflow_logger = logging.getLogger("Accum.Signal.Realtime")
        self.telegram = TelegramNotifier(settings.telegram_token, settings.telegram_chat_id)
        self.realtime_engine = RealtimeAccumulationEngine(settings)
        self.macro_engine = MacroAccumulationEngine(settings)
        self.csv_logger = SignalCsvLogger("accumulation_signals.csv")
        self.rejection_logger = RejectionCsvLogger("rejection_reasons.csv")
        self.signal_store = SignalStore()
        self.dashboard = DashboardIngestClient()
        self._cooldowns: dict[str, float] = {}
        self._counts = {"macro": 0, "orderflow": 0}
        self._preimpulse_kinds = {
            "ACCUMULATION_WATCH",
            "ABSORPTION_ZONE",
            "PRE_IMPULSE_ZONE",
            "BREAKOUT_PRESSURE",
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
        async with BybitRestClient(self.settings.rest_base_url, timeout_seconds=self.settings.rest_timeout_seconds, retries=self.settings.rest_retries) as rest:
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
            self.ui.update_session(ws_status=stream.status, macro=self._counts["macro"], orderflow=self._counts["orderflow"])
            self.ui.print_session(realtime_count, macro_count)
            await asyncio.sleep(30)

        async def _run_realtime_scan(self, rest: BybitRestClient, stream: MarketStream, symbols: list[ScanTarget]) -> None:
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
                        signals = self.realtime_engine.analyze(symbol, df, state)
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

                            await self._emit_signal(rest, signal)
                except Exception as exc:
                    self.logger.warning("Realtime scan failed for %s: %r", symbol, exc)
                await asyncio.sleep(0.05)
            await asyncio.sleep(max(self.settings.realtime_scan_every_seconds, 1))

    async def _run_macro_scan(self, rest: BybitRestClient, symbols: list[ScanTarget]) -> None:
        self.logger.info("Macro base scan loop started for %s symbols", len(symbols))
        intervals = {"60": 60, "240": 50, "D": 45}
        while True:
            await self.dashboard.post_heartbeat("scanner", meta={"runner": "orderflow_accum", "loop": "macro", "symbols": len(symbols)})
            for target in symbols:
                try:
                    symbol = target.symbol
                    frames = {}
                    for interval, limit in intervals.items():
                        frames[interval] = await rest.fetch_klines(symbol, interval=interval, limit=limit, category=target.market)
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
            df = await rest.fetch_klines(signal.symbol, interval=interval, limit=bars, category=market)
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

    async def _emit_signal(self, rest: BybitRestClient, signal) -> None:
        market = str(signal.meta.get("market", self.settings.market_categories[0].lower() if self.settings.market_categories else "linear"))
        upsert = self.signal_store.upsert_signal(signal, market=market)
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
        if not upsert.should_notify:
            return
        # Policy: CSV/UI are notify-only to avoid repeat-noise pollution.
        self.csv_logger.append(signal)
        self.ui.update_session(macro=self._counts["macro"], orderflow=self._counts["orderflow"])
        self.ui.print_signal(signal)
        if not upsert.should_notify:
            return
        await self.dashboard.post_signal(signal)
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
