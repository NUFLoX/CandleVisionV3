# -*- coding: utf-8 -*-
import logging
import pandas as pd
from api.market import fetch_ohlcv_bybit_async

class Sentinel:
    def __init__(self):
        self.logger = logging.getLogger("CandleVision.Sentinel")
        self.symbol = "BTCUSDT"

    async def get_market_regime_async(self) -> str:
        """Определяет текущий контекст рынка по BTC."""
        try:
            # Скачиваем последние 50 свечей битка (таймфрейм 15 минут)
            df = await fetch_ohlcv_bybit_async(self.symbol, "15m", 50)
            if df.empty or len(df) < 50:
                self.logger.warning("⚠️ Sentinel не получил данные BTC. Режим DANGER.")
                return "DANGER"

            close_price = df['close'].iloc[-1]
            
            # Считаем EMA50 (Глобальный тренд за 12 часов)
            ema50 = df['close'].ewm(span=50, adjust=False).mean().iloc[-1]
            
            # Считаем волатильность: разница между хаем и лоу за последние 10 свечей (2.5 часа)
            high_10 = df['high'].tail(10).max()
            low_10 = df['low'].tail(10).min()
            volatility_pct = ((high_10 - low_10) / low_10) * 100

            # 1. Проверка на шторм (Flash Crash / Pump)
            if volatility_pct > 3.5: # Если биток скачет больше 3.5% - это опасно
                self.logger.warning(f"🚨 BTC ШТОРМИТ (Волатильность {volatility_pct:.2f}%). Режим DANGER.")
                return "DANGER"

            # 2. Определение тренда
            if close_price > ema50 * 1.002: # Цена уверенно выше EMA
                return "BULL"
            elif close_price < ema50 * 0.998: # Цена уверенно ниже EMA
                return "BEAR"
            else:
                return "FLAT" # Цена прилипла к EMA

        except Exception as e:
            self.logger.error(f"❌ Ошибка в Sentinel: {e}")
            return "DANGER" # При любой ошибке уходим в защиту