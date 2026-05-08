# -*- coding: utf-8 -*-
import asyncio
import logging

from config.settings import SCAN_INTERVAL, WATCHLIST_INTERVAL, DANGER_PAUSE
from dashboard.ingest_client import DashboardIngestClient

class Orchestrator:
    def __init__(self, scout, executor, sentinel, ws_stream):
        self.scout = scout
        self.executor = executor
        self.sentinel = sentinel
        self.ws_stream = ws_stream
        self.is_running = False
        self.logger = logging.getLogger("CandleVision.Orchestrator")
        self.dashboard = DashboardIngestClient()

    async def run_scout_loop(self):
        self.logger.info("🔭 Запуск асинхронного Сканера...")
        while self.is_running:
            await self.dashboard.post_heartbeat("scanner", meta={"runner": "orchestrator", "loop": "scout"})
            regime = await self.sentinel.get_market_regime_async()

            if regime == "DANGER":
                self.logger.warning(f"⚠️ Рынок нестабилен. Пауза {DANGER_PAUSE} сек.")
                await asyncio.sleep(DANGER_PAUSE)
            else:
                self.logger.info(f"🚦 Режим рынка: {regime}. Запускаем сканирование...")
                await self.scout.run_full_market_scan_async(regime)
                await asyncio.sleep(SCAN_INTERVAL)

    async def run_executor_loop(self):
        self.logger.info("⚡ Запуск асинхронного Экзекутора...")
        while self.is_running:
            await self.dashboard.post_heartbeat("executor", meta={"runner": "orchestrator", "loop": "executor", "open_trades": len(getattr(self.executor, 'active_trades', []))})
            try:
                signal = await asyncio.wait_for(self.executor.queue.get(), timeout=5)
            except asyncio.TimeoutError:
                continue
            if signal:
                await self.executor.process_signal_async(signal)
            await asyncio.sleep(0.1)

    async def run_watchlist_refiner(self):
        """Цикл 'Дожима': перепроверяет Watchlist."""
        self.logger.info("🎯 Запуск цикла 'Дожима' (Watchlist Refiner)...")
        while self.is_running:
            await self.dashboard.post_heartbeat("scanner", meta={"runner": "orchestrator", "loop": "watchlist_refiner", "watchlist_size": len(self.executor.watchlist)})
            targets = list(self.executor.watchlist)
            if targets:
                await self.scout.recheck_watchlist_async(targets)
            await asyncio.sleep(WATCHLIST_INTERVAL)

    async def run_position_monitor(self):
        """
        Мониторинг открытых позиций для локальной логики (TP1 и синхронизация БД).
        Берет точные цены Best Bid прямо из WebSockets стакана.
        """
        self.logger.info("🛡️ Запуск монитора позиций (TP1/Синхронизация)...")
        await asyncio.sleep(10)  # Ждём прогрева стаканов
        while self.is_running:
            await self.dashboard.post_heartbeat("executor", meta={"runner": "orchestrator", "loop": "position_monitor", "open_trades": len(getattr(self.executor, 'active_trades', []))})
            try:
                price_updates = {}
                for trade in self.executor.active_trades:
                    if trade['status'] == 'open':
                        symbol = trade['symbol']
                        
                        # ИСПРАВЛЕНИЕ: Используем новый метод получения стакана
                        book = self.ws_stream.get_orderbook(symbol)
                        if book and book.get("bids"):
                            # bids[0] - это лучший покупатель, bids[0][0] - его цена
                            best_bid_price = float(book["bids"][0][0])
                            price_updates[symbol] = best_bid_price
                
                if price_updates:
                    await self.executor.update_positions(price_updates)
            except Exception as e:
                self.logger.error(f"❌ Ошибка монитора позиций: {e}")
            
            # 5 секунд достаточно для проверки TP1, остальное контролирует сама биржа
            await asyncio.sleep(5) 

    async def start(self):
        self.is_running = True
        tasks = [
            asyncio.create_task(self.ws_stream.connect(self.scout.symbols), name="ws_stream"),
            asyncio.create_task(self.run_scout_loop(), name="scout_loop"),
            asyncio.create_task(self.run_executor_loop(), name="executor_loop"),
            asyncio.create_task(self.run_watchlist_refiner(), name="watchlist_refiner"),
            asyncio.create_task(self.run_position_monitor(), name="position_monitor"),
        ]

        if getattr(self.scout, 'tg_agent', None):
            tasks.append(asyncio.create_task(self.scout.tg_agent.start_listening(self.scout.symbols), name="telegram_scout"))

        if getattr(self.scout, 'sentiment_agent', None):
            tasks.append(asyncio.create_task(self.scout.sentiment_agent.start_polling(self.scout.symbols), name="sentiment_agent"))

        if getattr(self.scout, 'tape_agent', None):
            tasks.append(asyncio.create_task(self.scout.tape_agent.connect(self.scout.symbols), name="tape_agent"))

        try:
            await asyncio.gather(*tasks)
        finally:
            self.is_running = False
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self):
        self.is_running = False
