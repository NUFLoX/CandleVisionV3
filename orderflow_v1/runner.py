from __future__ import annotations

import asyncio
import logging
import time

from .bybit_rest import BybitRestClient
from .console_ui import ConsoleUI
from .config import Settings
from .engines import MacroFlowEngine, RealtimeOrderFlowEngine
from .signal_logger import SignalCsvLogger
from .telegram_notify import TelegramNotifier
from .ws_clients import MarketStream


class OrderFlowRunner:
    def __init__(self, settings: Settings, ui: ConsoleUI | None = None, version: str = "V1.3.1 WS"):
        self.settings = settings
        self.ui = ui or ConsoleUI()
        self.version = version
        self.logger = logging.getLogger("OrderFlow.Runner")
        self.macro_logger = logging.getLogger("OrderFlow.Signal.Macro")
        self.orderflow_logger = logging.getLogger("OrderFlow.Signal.Realtime")
        self.telegram = TelegramNotifier(settings.telegram_token, settings.telegram_chat_id)
        self.realtime_engine = RealtimeOrderFlowEngine(settings)
        self.macro_engine = MacroFlowEngine(settings)
        self.csv_logger = SignalCsvLogger("signal_stats.csv")
        self._cooldowns: dict[str, float] = {}

    async def run(self) -> None:
        async with BybitRestClient(self.settings.rest_base_url) as rest:
            realtime_symbols = await rest.fetch_best_symbols(
                quote_coin=self.settings.quote_coin,
                limit=self.settings.realtime_symbols_limit,
                min_notional_24h=self.settings.min_notional_24h,
                min_last_price=self.settings.min_last_price,
                allowlist=self.settings.symbols_allowlist,
                blocklist=self.settings.symbols_blocklist,
            )
            macro_symbols = await rest.fetch_best_symbols(
                quote_coin=self.settings.quote_coin,
                limit=self.settings.macro_symbols_limit,
                min_notional_24h=self.settings.min_notional_24h,
                min_last_price=self.settings.min_last_price,
                allowlist=self.settings.symbols_allowlist,
                blocklist=self.settings.symbols_blocklist,
            )

            stream = MarketStream(
                url=self.settings.ws_public_url,
                book_depth=self.settings.book_depth,
                tape_window_seconds=self.settings.tape_window_seconds,
                wall_persistence_seconds=self.settings.wall_persistence_seconds,
                heartbeat_seconds=self.settings.ws_heartbeat_seconds,
            )
            await self.telegram.send_message(
                f"🚀 <b>OrderFlow {self.version} started</b>\n"
                f"Realtime symbols: {len(realtime_symbols)}\n"
                f"Macro symbols: {len(macro_symbols)}\n"
                f"Mode: {'signals only' if self.settings.signals_only else 'trade ready'}"
            )

            tasks = [
                asyncio.create_task(stream.run(realtime_symbols), name="orderflow_ws"),
                asyncio.create_task(self._run_realtime_scan(rest, stream, realtime_symbols), name="orderflow_realtime"),
                asyncio.create_task(self._run_macro_scan(rest, macro_symbols), name="orderflow_macro"),
            ]
            await asyncio.gather(*tasks)

    async def _run_realtime_scan(self, rest: BybitRestClient, stream: MarketStream, symbols: list[str]) -> None:
        self.logger.info("Realtime scan loop started for %s symbols", len(symbols))
        while True:
            for symbol in symbols:
                try:
                    df = await rest.fetch_klines(symbol, interval="1", limit=180)
                    state = stream.get_state(symbol)
                    signals = self.realtime_engine.analyze(symbol, df, state)
                    for signal in signals:
                        await self._emit_signal(signal)
                except Exception as exc:
                    self.logger.warning("Realtime scan failed for %s: %s", symbol, exc)
                await asyncio.sleep(0.06)
            await asyncio.sleep(max(self.settings.scan_every_seconds, 1))

    async def _run_macro_scan(self, rest: BybitRestClient, symbols: list[str]) -> None:
        self.logger.info("Macro scan loop started for %s symbols", len(symbols))
        intervals = {"60": 48, "240": 42, "D": 40}
        while True:
            for symbol in symbols:
                try:
                    frames = {}
                    for interval, limit in intervals.items():
                        frames[interval] = await rest.fetch_klines(symbol, interval=interval, limit=limit)
                        await asyncio.sleep(0.04)
                    signal = self.macro_engine.analyze(symbol, frames)
                    if signal:
                        await self._emit_signal(signal)
                except Exception as exc:
                    self.logger.warning("Macro scan failed for %s: %s", symbol, exc)
                await asyncio.sleep(0.08)
            await asyncio.sleep(max(self.settings.macro_every_seconds, 60))

    def _cooldown_seconds(self, signal) -> int:
        if signal.source == "macro":
            return self.settings.macro_symbol_cooldown_minutes * 60
        return self.settings.signal_cooldown_seconds

    def _should_emit(self, signal) -> bool:
        key = f"{signal.source}:{signal.dedupe_key()}"
        now = time.time()
        expires_at = self._cooldowns.get(key, 0.0)
        if expires_at > now:
            return False
        self._cooldowns[key] = now + self._cooldown_seconds(signal)
        return True

    async def _emit_signal(self, signal) -> None:
        if not self._should_emit(signal):
            return
        message = self._format_signal(signal)
        log_line = message.replace("<b>", "").replace("</b>", "")
        if signal.source == "macro":
            self.macro_logger.info(log_line)
        else:
            self.orderflow_logger.info(log_line)
        self.csv_logger.append(signal)
        self.ui.print_signal(signal)
        await self.telegram.send_message(message)

    def _format_signal(self, signal) -> str:
        reasons = ", ".join(signal.reasons[:6])
        meta = ", ".join(f"{key}={value}" for key, value in list(signal.meta.items())[:6])
        header = "MACRO_SIGNAL" if signal.source == "macro" else "ORDERFLOW_SIGNAL"
        return (
            f"📡 <b>{header}</b>\n"
            f"{signal.kind}\n"
            f"#{signal.symbol} | {signal.side} | score={signal.score}\n"
            f"entry={signal.entry:.8f}\n"
            f"sl={signal.stop_loss:.8f}\n"
            f"tp1={signal.take_profit_1:.8f}\n"
            f"tp2={signal.take_profit_2:.8f}\n"
            f"reasons: {reasons}\n"
            f"meta: {meta}"
        )
