# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import logging
import time
from config.settings import BYBIT_REST_BASE_URL

class VolumeSonar:
    def __init__(self, notifier, min_24h_vol=1000000, rvol_threshold=5.0):
        self.logger = logging.getLogger("CandleVision.Sonar")
        self.notifier = notifier
        self.min_24h_vol = min_24h_vol      # Отсекаем мусор < $1M
        self.rvol_threshold = rvol_threshold # Порог суеты: 5.0x (500% всплеск)
        self.history = {} # Память для снимков: {symbol: {volume, timestamp}}
        self.api_url = f"{BYBIT_REST_BASE_URL}/v5/market/tickers?category=linear"

    async def fetch_tickers(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.api_url) as response:
                    data = await response.json()
                    return data.get("result", {}).get("list", [])
        except Exception as e:
            self.logger.error(f"❌ Ошибка Сонара при получении тикеров: {e}")
            return []

    async def scan_market(self, scout):
        """Делает снимок и ищет аномалии"""
        tickers = await self.fetch_tickers()
        new_hot_coins = []

        for t in tickers:
            symbol = t["symbol"]
            if not symbol.endswith("USDT"): 
                continue
                
            vol_24h = float(t.get("turnover24h", 0))
            if vol_24h < self.min_24h_vol:
                continue

            normal_15m_vol = vol_24h / 96

            if symbol in self.history:
                last_vol = self.history[symbol]["volume"]
                real_15m_vol = vol_24h - last_vol
                
                if real_15m_vol > 0 and normal_15m_vol > 0:
                    rvol = real_15m_vol / normal_15m_vol
                    
                    # 🚨 СОНАР НАШЕЛ АНОМАЛИЮ
                    if rvol >= self.rvol_threshold:
                        self.logger.warning(f"🚨 СОНАР {symbol}: Всплеск {rvol:.1f}x | Влито: ${real_15m_vol:,.0f}")
                        new_hot_coins.append(symbol)
                        
                        msg = (
                            f"🚨 <b>СОНАР: АНОМАЛИЯ ОБЪЕМА</b>\n"
                            f"🪙 <b>Монета:</b> {symbol}\n"
                            f"📈 <b>Всплеск:</b> {rvol:.1f}x\n"
                            f"💰 <b>Влито за 15м:</b> ${real_15m_vol:,.0f}\n"
                            f"🎯 <i>Беру на прицел Сканера!</i>"
                        )
                        await self.notifier.send_message(msg)

            self.history[symbol] = {
                "volume": vol_24h,
                "timestamp": time.time()
            }
            
        # Динамическая подмена монет в Сканере
        if new_hot_coins and scout:
            for coin in new_hot_coins:
                if coin not in scout.symbols:
                    scout.symbols.pop() # Убираем с конца списка самую тухлую
                    scout.symbols.insert(0, coin) # Ставим горячую в начало
            self.logger.info(f"🔄 Горячая замена! Новые монеты в фокусе: {', '.join(new_hot_coins)}")

    async def run_loop(self, scout, tape_agent):
        """Бесконечный фоновый процесс (каждые 15 минут)"""
        self.logger.info("📡 Volume Sonar запущен. Накапливаю базовую память...")
        await self.scan_market(scout=None) # Сначала делаем слепой снимок базы
        
        while True:
            await asyncio.sleep(900) # Спим 15 минут (900 секунд)
            self.logger.info("🔍 Сонар сканирует глубины рынка...")
            await self.scan_market(scout)