# -*- coding: utf-8 -*-
import logging
import asyncio  # <--- ДОБАВИЛИ ИМПОРТ ДЛЯ АСИНХРОННОСТИ
from config.settings import BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_TESTNET

class BybitClient:
    def __init__(self, testnet=None):
        self.logger = logging.getLogger("CandleVision.Bybit")
        
        if testnet is None:
            testnet = BYBIT_TESTNET
        api_key = BYBIT_API_KEY
        api_secret = BYBIT_API_SECRET

        if not api_key or not api_secret:
            self.logger.warning(
                "⚠️ Ключи Bybit не найдены в .env! "
                "Бот работает в режиме 'Только сигналы'."
            )
            self.session = None
            return

        from pybit.unified_trading import HTTP

        try:
            # Классическое подключение. testnet=True направит нас на testnet.bybit.com
            self.session = HTTP(
                testnet=testnet,
                api_key=api_key,
                api_secret=api_secret,
            )
            
            mode_name = "📝 Testnet" if testnet else "🚀 Mainnet (LIVE!)"
            self.logger.info(f"✅ Успешное подключение к Bybit API ({mode_name})")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка авторизации Bybit: {e}")
            self.session = None

    # =================================================================
    # НОВЫЙ МЕТОД ДЛЯ МАКРО-ТРЕКЕРА (СКАЧИВАНИЕ СВЕЧЕЙ)
    # =================================================================
    async def get_kline(self, symbol: str, interval: str, limit: int = 50, start: int = None):
        """Асинхронная обертка для получения свечей через pybit v5"""
        if not self.session:
            self.logger.warning("⚠️ Session не инициализирован. Не могу получить свечи.")
            return None

        try:
            # Запускаем синхронный запрос pybit в отдельном потоке, 
            # чтобы не блокировать асинхронный цикл бота
            loop = asyncio.get_event_loop()
            
            kwargs = {
                "category": "linear",
                "symbol": symbol,
                "interval": str(interval),
                "limit": limit
            }
            if start is not None:
                kwargs["start"] = start

            response = await loop.run_in_executor(
                None, 
                lambda: self.session.get_kline(**kwargs)
            )
            
            return response.get('result', {})
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка get_kline для {symbol}: {e}")
            return None
    # =================================================================

    def execute_long_trade(
        self, 
        symbol: str, 
        qty: float, 
        entry_price: float, 
        sl_price: float, 
        tp_price: float
    ):
        """Открывает сделку с защитой SL/TP и трейлинг-стопом."""
        if not self.session:
            self.logger.warning(f"⚠️ Session не инициализирован. Пропускаем ордер {symbol}")
            return False

        try:
            # 1️⃣ ОТКРЫВАЕМ ПОЗИЦИЮ
            order_response = self.session.place_order(
                category="linear",
                symbol=symbol,
                side="Buy",              # LONG
                orderType="Market",      # Гарантированный вход
                qty=str(qty),
                stopLoss=str(sl_price),
                takeProfit=str(tp_price),
                positionIdx=0            # One-Way Mode
            )
            
            order_id = order_response.get('result', {}).get('orderId', 'N/A')
            self.logger.info(
                f"🟢 ВХОД выполнен! "
                f"Symbol: {symbol} | "
                f"Qty: {qty} | "
                f"OrderID: {order_id}"
            )

            # 2️⃣ НАСТРАИВАЕМ ТРЕЙЛИНГ-СТОП
            activation_price = entry_price * 1.05  # Активация +5%
            callback_dist = entry_price * 0.01      # Отката 1% от входа

            self.session.set_trading_stop(
                category="linear",
                symbol=symbol,
                trailingStop=str(round(callback_dist, 4)),
                activePrice=str(round(activation_price, 4)),
                positionIdx=0
            )
            
            self.logger.info(
                f"🎣 Трейлинг-стоп установлен | "
                f"Активация: {activation_price:.4f} | "
                f"Шаг: {callback_dist:.4f}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"❌ Ошибка выставления ордера {symbol}: {e}"
            )
            return False