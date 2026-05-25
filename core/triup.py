# -*- coding: utf-8 -*-
import asyncio
from config.settings import trading_enabled
import logging
from datetime import datetime, timedelta

class TriUpManager:
    def __init__(self, api_client, db, telegram_bot=None):
        self.logger = logging.getLogger("CandleVision.triUP")
        self.api = api_client
        self.db = db
        self.telegram = telegram_bot
        
        # --- НАСТРОЙКИ ЗАЩИТЫ ---
        self.breakeven_trigger_pct = 1.5  # При +1.5% профита переводим в безубыток
        self.fee_buffer_pct = 0.15        # Добавляем 0.15% к цене входа, чтобы покрыть комиссии биржи
        
        # --- НАСТРОЙКИ АНАЛИТИКИ ---
        self.report_hour = 8              # Время отправки отчета (08:00 утра по времени сервера)
        self.last_report_date = None

    async def _check_breakeven(self):
        """Проверяет открытые позиции и переводит в Б/У, если достигнут таргет"""
        if not self.api.session:
            return

        try:
            # Получаем все открытые позиции с биржи
            # Запускаем в отдельном потоке, чтобы не блокировать event loop
            loop = asyncio.get_event_loop()
            positions_response = await loop.run_in_executor(
                None, 
                lambda: self.api.session.get_positions(category="linear", settleCoin="USDT")
            )
            
            positions = positions_response.get('result', {}).get('list', [])
            
            for pos in positions:
                size = float(pos.get('size', 0))
                if size == 0:
                    continue # Позиция закрыта
                    
                symbol = pos['symbol']
                side = pos['side']
                entry_price = float(pos['avgPrice'])
                current_price = float(pos['markPrice'])
                current_sl = float(pos.get('stopLoss', 0))
                
                # Логика для LONG позиций
                if side == "Buy":
                    profit_pct = ((current_price - entry_price) / entry_price) * 100
                    breakeven_price = entry_price * (1 + (self.fee_buffer_pct / 100))
                    
                    # Если профит больше 1.5% И стоп-лосс еще не переведен в Б/У
                    if profit_pct >= self.breakeven_trigger_pct and current_sl < entry_price:
                        self.logger.info(f"🛡️ triUP: {symbol} дал +{profit_pct:.2f}%. Перевожу в БЕЗУБЫТОК!")
                        
                        # Обновляем Stop-Loss на бирже
                        await loop.run_in_executor(
                            None,
                            lambda: self.api.session.set_trading_stop(
                                category="linear",
                                symbol=symbol,
                                stopLoss=str(round(breakeven_price, 4)),
                                positionIdx=0
                            )
                        )
                        
                        msg = f"🛡 <b>Защита triUP сработала!</b>\nМонета: #{symbol}\nСделка в БЕЗУБЫТКЕ (SL: {breakeven_price:.4f})"
                        if self.telegram:
                            await self.telegram.send_message(msg)
                            
        except Exception as e:
            self.logger.error(f"Ошибка в модуле защиты Б/У: {e}")

    async def _send_morning_report(self):
        """Собирает статистику за прошлые сутки и отправляет в TG"""
        now = datetime.now()
        
        # Проверяем, наступило ли 8 утра и не отправляли ли мы уже отчет сегодня
        if now.hour == self.report_hour and self.last_report_date != now.date():
            self.logger.info("📊 triUP: Подготовка утренней аналитики...")
            
            # Здесь в будущем мы прикрутим SQL-запрос к self.db, чтобы достать стату за 24 часа.
            # Пока сделаем заглушку для проверки системы.
            report_msg = (
                f"🌅 <b>Утренняя аналитика CandleVision</b>\n"
                f"📅 Дата: {now.strftime('%Y-%m-%d')}\n\n"
                f"⚙️ Система работает стабильно.\n"
                f"🛡️ Модуль защиты капитала (triUP) активен.\n"
                f"🔭 SmartMoney Снайпер на дежурстве.\n\n"
                f"<i>Хорошего и профитного дня!</i> ☕"
            )
            
            if self.telegram:
                await self.telegram.send_message(report_msg)
                
            self.last_report_date = now.date()

    async def run_loop(self):
        """Бесконечный цикл работы модуля triUP"""
        self.logger.info("🛡️ Модуль triUP (Защита и Аналитика) успешно запущен.")
        
        while True:
            if not trading_enabled():
                await asyncio.sleep(10)
                continue
            # 1. Проверяем безубыток (каждые 10 секунд)
            await self._check_breakeven()
            
            # 2. Проверяем, не пора ли слать утренний отчет
            await self._send_morning_report()
            
            # Спим 10 секунд
            await asyncio.sleep(10)