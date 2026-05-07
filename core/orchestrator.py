# -*- coding: utf-8 -*-
import asyncio
import logging

from config.settings import SCAN_INTERVAL, WATCHLIST_INTERVAL, DANGER_PAUSE

class Orchestrator:
    def __init__(self, scout, executor, sentinel, ws_stream):
        self.scout = scout
        self.executor = executor
        self.sentinel = sentinel
        self.ws_stream = ws_stream
        self.is_running = False
        self.logger = logging.getLogger("CandleVision.Orchestrator")

    async def run_scout_loop(self):
        self.logger.info("🔭 Запуск асинхронного Сканера...")
        while self.is_running:
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
            signal = await self.executor.queue.get()
            if signal:
                await self.executor.process_signal_async(signal)
            await asyncio.sleep(0.1)

    async def run_watchlist_refiner(self):
        """Цикл 'Дожима': перепроверяет Watchlist."""
        self.logger.info("🎯 Запуск цикла 'Дожима' (Watchlist Refiner)...")
        while self.is_running:
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
        
        
        # ЗАТЕМ запускаем все процессы, передавая этот список в WebSocket
        await asyncio.gather(
            self.ws_stream.connect(self.scout.symbols), # <-- Передали монеты сюда!
            self.run_scout_loop(),
            self.run_executor_loop(),
            self.run_watchlist_refiner(),
            self.run_position_monitor() 
        )

        # Если Telegram-агент подключен, запускаем его фоновую прослушку
        if getattr(self.scout, 'tg_agent', None):
            tasks.append(self.scout.tg_agent.start_listening(self.scout.symbols))

        await asyncio.gather(*tasks)

        if getattr(self.scout, 'sentiment_agent', None):
            tasks.append(self.scout.sentiment_agent.start_polling(self.scout.symbols))

        # Запуск прослушки ленты сделок (Киты)
        if getattr(self.scout, 'tape_agent', None):
            tasks.append(self.scout.tape_agent.connect(self.scout.symbols))

        await asyncio.gather(*tasks)

    def stop(self):
        self.is_running = False