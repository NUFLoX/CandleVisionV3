# -*- coding: utf-8 -*-
import logging
import asyncio  # <--- ДОБАВИЛИ ИМПОРТ ДЛЯ АСИНХРОННОСТИ
from config.settings import BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_TESTNET, trading_enabled

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


    def get_instrument_rules(self, symbol: str) -> dict | None:
        """Fetch Bybit linear instrument filters needed before order placement."""
        if not self.session:
            return None
        try:
            response = self.session.get_instruments_info(category="linear", symbol=symbol)
            if response.get("retCode") != 0:
                self.logger.error(f"❌ Ошибка instrument rules для {symbol}: {response}")
                return None
            rows = response.get("result", {}).get("list", [])
            if not rows:
                self.logger.error(f"❌ Bybit не вернул instrument rules для {symbol}")
                return None
            row = rows[0]
            lot = row.get("lotSizeFilter", {})
            price = row.get("priceFilter", {})
            return {
                "symbol": symbol,
                "tick_size": str(price.get("tickSize") or "0"),
                "qty_step": str(lot.get("qtyStep") or "0"),
                "min_qty": str(lot.get("minOrderQty") or "0"),
                "min_notional": str(lot.get("minNotionalValue") or "0"),
            }
        except Exception as e:
            self.logger.error(f"❌ Ошибка instrument rules для {symbol}: {e}")
            return None

    def get_open_orders(self, symbol: str | None = None, order_link_id: str | None = None) -> list[dict]:
        if not self.session:
            return []
        try:
            kwargs = {"category": "linear"}
            if symbol:
                kwargs["symbol"] = symbol
            if order_link_id:
                kwargs["orderLinkId"] = order_link_id
            response = self.session.get_open_orders(**kwargs)
            if response.get("retCode") != 0:
                self.logger.error(f"❌ Ошибка open orders: {response}")
                return []
            return response.get("result", {}).get("list", []) or []
        except Exception as e:
            self.logger.error(f"❌ Ошибка open orders: {e}")
            return []

    def get_positions(self, symbol: str | None = None) -> list[dict]:
        if not self.session:
            return []
        try:
            kwargs = {"category": "linear", "settleCoin": "USDT"}
            if symbol:
                kwargs = {"category": "linear", "symbol": symbol}
            response = self.session.get_positions(**kwargs)
            if response.get("retCode") != 0:
                self.logger.error(f"❌ Ошибка positions: {response}")
                return []
            return response.get("result", {}).get("list", []) or []
        except Exception as e:
            self.logger.error(f"❌ Ошибка positions: {e}")
            return []

    def has_open_position(self, symbol: str) -> bool:
        for position in self.get_positions(symbol):
            try:
                if float(position.get("size") or 0.0) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def get_order_status(self, symbol: str, order_id: str | None = None, order_link_id: str | None = None) -> dict | None:
        orders = self.get_open_orders(symbol=symbol, order_link_id=order_link_id)
        if order_id:
            for order in orders:
                if order.get("orderId") == order_id:
                    return order
        if orders:
            return orders[0]
        try:
            kwargs = {"category": "linear", "symbol": symbol, "limit": 20}
            if order_id:
                kwargs["orderId"] = order_id
            if order_link_id:
                kwargs["orderLinkId"] = order_link_id
            response = self.session.get_order_history(**kwargs)
            if response.get("retCode") != 0:
                return None
            rows = response.get("result", {}).get("list", []) or []
            return rows[0] if rows else None
        except Exception as e:
            self.logger.error(f"❌ Ошибка order status для {symbol}: {e}")
            return None

    # =================================================================
    # НОВЫЙ МЕТОД ДЛЯ МАКРО-ТРЕКЕРА (СКАЧИВАНИЕ СВЕЧЕЙ)
    # =================================================================
    async def get_kline(self, symbol: str, interval: str, limit: int = 50, start: int = None, end: int = None):
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
            if end is not None:
                kwargs["end"] = end

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
        if not trading_enabled():
            self.logger.warning(f"🛡️ trading_enabled=false. Пропускаем execute_long_trade для {symbol}")
            return False
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
