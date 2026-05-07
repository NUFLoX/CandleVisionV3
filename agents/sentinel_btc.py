# -*- coding: utf-8 -*-
import logging
from api.market import fetch_ohlcv_bybit

class SentinelBTC:
    """
    Глобальный страж рынка.
    Следит за Биткоином. Если BTC резко падает, запрещает торговлю альтами.
    """
    def __init__(self):
        self.logger = logging.getLogger("CandleVision.Sentinel")

    def is_market_safe(self) -> bool:
        """Возвращает True, если рынок стабилен, и False, если паника."""
        # Берем последние свечи BTC
        df = fetch_ohlcv_bybit("BTCUSDT", "15m", limit=10)
        
        if df.empty:
            self.logger.warning("⚠️ Ошибка связи с Bybit при проверке BTC. Включаю защиту.")
            return False

        last_close = float(df["close"].iloc[-1])
        # Смотрим цену открытия 4 свечи назад (ровно 1 час)
        hour_ago_open = float(df["open"].iloc[-4])
        
        # Считаем процент падения
        drop_pct = ((hour_ago_open - last_close) / hour_ago_open) * 100

        # Если биток упал больше чем на 1.5% за час - красный свет
        if drop_pct > 1.5:
            self.logger.warning(f"🚨 ПАДЕНИЕ BTC НА {drop_pct:.2f}%! Режим защиты активирован.")
            return False
            
        self.logger.info("✅ Глобальный фон (BTC) стабилен.")
        return True