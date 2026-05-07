# -*- coding: utf-8 -*-
import asyncio
import logging
import time

class SmartMoneyTracker:
    def __init__(self, api_client, scan_interval_hours=2):
        self.logger = logging.getLogger("CandleVision.SmartMoney")
        self.api = api_client
        self.scan_interval = scan_interval_hours * 3600
        
        # Сетка таймфреймов: "интервал Bybit", "кол-во свечей", "макс. ширина коридора", "мин. объем USDT"
        self.timeframes = {
            "1H": {"interval": "60", "limit": 24, "max_range": 3.0, "min_vol": 2_000_000},   # Внутри дня (сутки)
            "4H": {"interval": "240", "limit": 30, "max_range": 6.0, "min_vol": 10_000_000}, # Среднесрок (5 дней)
            "1D": {"interval": "D", "limit": 30, "max_range": 15.0, "min_vol": 30_000_000},  # Месячное накопление
            "1W": {"interval": "W", "limit": 12, "max_range": 25.0, "min_vol": 80_000_000}   # Глобальное (3 месяца)
        }

    async def _fetch_data(self, symbol, interval, limit):
        """Скачивает свечи нужного таймфрейма"""
        try:
            response = await self.api.get_kline(symbol=symbol, interval=interval, limit=limit)
            if not response or 'list' not in response:
                return None
            return response['list']
        except Exception as e:
            self.logger.error(f"Ошибка загрузки {symbol} ({interval}): {e}")
            return None

    def _analyze_accumulation(self, symbol, tf_name, klines, config):
        if not klines or len(klines) < config["limit"] // 2:
            return None

        highs = [float(c[2]) for c in klines]
        lows = [float(c[3]) for c in klines]
        total_volume_usd = sum(float(c[6]) for c in klines) # c[6] - turnover (USDT)

        macro_high = max(highs)
        macro_low = min(lows)
        current_price = float(klines[0][4])

        range_pct = ((macro_high - macro_low) / macro_low) * 100

        # Проверяем условия накопления для конкретного таймфрейма
        if total_volume_usd > config["min_vol"] and range_pct < config["max_range"]:
            distance_to_breakout = ((macro_high - current_price) / current_price) * 100
            
            # Бьем тревогу, если цена уже поджалась к потолку (менее 3% до пробоя)
            if distance_to_breakout < 3.0:
                density = total_volume_usd / max(range_pct, 0.1) # Плотность: Объем на 1% движения
                return {
                    "symbol": symbol,
                    "tf": tf_name,
                    "volume_usd": total_volume_usd,
                    "range_pct": range_pct,
                    "resistance": macro_high,
                    "score": round(density, 2)
                }
        return None

    async def run_loop(self, active_symbols, ws_tape_reader, telegram_bot):
        self.logger.info("🔭 SmartMoney Tracker запущен. Фрактальное сканирование (1H, 4H, 1D, 1W)...")
        
        while True:
            targets = []
            
            for symbol in active_symbols:
                for tf_name, config in self.timeframes.items():
                    klines = await self._fetch_data(symbol, config["interval"], config["limit"])
                    result = self._analyze_accumulation(symbol, tf_name, klines, config)
                    
                    if result:
                        targets.append(result)
                        # Если нашли на младшем ТФ, можно не проверять старшие, чтобы не дублировать
                        break 
                        
                await asyncio.sleep(0.1) # Пауза для API
            
            targets = sorted(targets, key=lambda x: x['score'], reverse=True)
            
            for t in targets:
                symbol = t['symbol']
                tf = t['tf']
                res_level = t['resistance']
                vol_m = t['volume_usd'] / 1_000_000
                
                msg = (
                    f"🐋 <b>НАКОПЛЕНИЕ ({tf})!</b>\n"
                    f"Монета: #{symbol}\n"
                    f"Влито: ${vol_m:.1f}M\n"
                    f"Коридор: {t['range_pct']:.1f}%\n"
                    f"Уровень пробоя: {res_level}\n\n"
                    f"⚡ <i>Снайпер (WS) взведен на уровень {res_level}</i>"
                )
                self.logger.warning(f"🐋 {tf} НАКОПЛЕНИЕ: {symbol} | ${vol_m:.1f}M | Пробой: {res_level}")
                
                if telegram_bot:
                    await telegram_bot.send_message(msg)
                
                if ws_tape_reader:
                    ws_tape_reader.add_sniper_target(symbol, res_level)
            
            self.logger.info(f"🔭 Цикл завершен. Сон {self.scan_interval / 3600} ч.")
            await asyncio.sleep(self.scan_interval)