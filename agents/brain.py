# -*- coding: utf-8 -*-
import asyncio
import logging
import sqlite3
import time

class AutoBrain:
    def __init__(self, db_path="candlevision.db", scan_interval_hours=72, min_trades_to_learn=10):
        self.logger = logging.getLogger("CandleVision.Brain")
        self.db_path = db_path
        self.scan_interval = scan_interval_hours * 3600 # в секундах (72 часа = 259200)
        self.min_trades = min_trades_to_learn # Не учимся, пока нет хотя бы 10 сделок
        
        # Базовые веса стратегий (будем их менять)
        self.weights = {
            "Pump": 1.0,
            "RSI": 0.5,
            "Whale": 1.5,
            "Sonar": 1.5
        }

        self.last_analyzed_id = 0

    def _get_closed_trades(self):
        """Достает закрытые сделки за последние 72 часа"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Берем только НОВЫЕ сделки (id > last_analyzed_id)
            cursor.execute("SELECT id, reasons, pnl_pct FROM trades WHERE status='closed' AND id > ? ORDER BY id ASC LIMIT 100", (self.last_analyzed_id,))
            rows = cursor.fetchall()
            conn.close()
            return rows
        except Exception as e:
            self.logger.error(f"❌ Ошибка Brain при доступе к БД: {e}")
            return []

    def _analyze_and_adjust(self):
        trades = self._get_closed_trades()
        
        if len(trades) < self.min_trades:
            self.logger.info(f"🧠 Brain: Мало НОВЫХ данных ({len(trades)}/{self.min_trades}). Жду.")
            return False

        # Обновляем закладку на ID самой последней выгруженной сделки (он лежит в row[0])
        self.last_analyzed_id = trades[-1][0] 

        stats = {}
        for row in trades:
            trade_id, reasons_str, pnl_pct = row[0], row[1], row[2] # <--- Сдвинули индексы, так как добавился ID
            # ... остальной код подсчета без изменений ...
            if not reasons_str: continue
            
            is_win = pnl_pct > 0
            
            # Разбиваем строку "Pump, Whale" на отдельные теги
            tags = [t.strip() for t in reasons_str.split(',')]
            
            for tag in tags:
                if tag not in stats:
                    stats[tag] = {"wins": 0, "total": 0}
                
                stats[tag]["total"] += 1
                if is_win:
                    stats[tag]["wins"] += 1

        self.logger.info("🧠 Brain: Анализ завершен. Результаты:")
        
        # Меняем веса на основе WinRate
        weights_changed = False
        for tag, data in stats.items():
            if data["total"] < 5: continue # Игнорируем случайные сделки
            
            winrate = (data["wins"] / data["total"]) * 100
            self.logger.info(f"   📊 {tag} | Сделок: {data['total']} | WinRate: {winrate:.1f}%")
            
            # Простая логика мутации весов
            # Если стратегия приносит убытки (< 40% побед), режем ее вес на 20%
            if winrate < 40.0 and tag in self.weights:
                self.weights[tag] = round(self.weights[tag] * 0.8, 2)
                weights_changed = True
                self.logger.warning(f"   📉 {tag} неэффективна! Вес снижен до {self.weights[tag]}")
            
            # Если стратегия рвет рынок (> 60% побед), повышаем ее вес на 20%
            elif winrate > 60.0 and tag in self.weights:
                # Ограничиваем максимальный вес, чтобы бот не сошел с ума
                new_weight = round(self.weights[tag] * 1.2, 2)
                if new_weight <= 3.0: 
                    self.weights[tag] = new_weight
                    weights_changed = True
                    self.logger.info(f"   📈 {tag} работает отлично! Вес повышен до {self.weights[tag]}")

        return weights_changed

    async def run_loop(self, scout):
        """Фоновый процесс самообучения"""
        self.logger.info(f"🧠 Модуль Самообучения запущен. Цикл анализа: {self.scan_interval / 3600} часов.")
        
        while True:
            # Спим заданное время (например, 72 часа)
            # Для тестов можешь поменять self.scan_interval на 60 (1 минута) в __init__
            await asyncio.sleep(self.scan_interval)
            
            self.logger.info("🧠 Brain проснулся. Начинаю анализ истории сделок...")
            changed = self._analyze_and_adjust()
            
            if changed and scout:
                # Внедряем новые веса в Сканер
                # Предполагается, что в Scout есть словарь self.strategy_weights
                scout.strategy_weights = self.weights.copy()
                self.logger.info("⚡ Новые нейронные связи сформированы. Веса переданы Сканеру.")
            else:
                self.logger.info("🧠 Текущие настройки оптимальны. Мутация не требуется.")