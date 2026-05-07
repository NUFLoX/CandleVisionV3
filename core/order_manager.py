# -*- coding: utf-8 -*-
import logging

class PaperTrader:
    """Виртуальный трейдер для тестирования стратегий без риска."""
    def __init__(self, initial_balance=1000.0):
        self.logger = logging.getLogger("CandleVision.PaperTrader")
        self.balance = initial_balance
        self.active_trades = {}  # Активные позиции
        self.history = []        # История сделок
        self.logger.info(f"💵 Paper Trader запущен. Стартовый баланс: {self.balance:.2f} USDT")

    def open_trade(self, symbol: str, entry_price: float, sl_pct: float, tp_pct: float):
        if symbol in self.active_trades:
            self.logger.warning(f"⚠️ Сделка по {symbol} уже открыта. Ждем закрытия.")
            return False

        # Инвестируем 10% от текущего баланса в одну сделку
        position_size_usdt = self.balance * 0.10 
        qty = position_size_usdt / entry_price

        # Точные цены выхода
        sl_price = entry_price * (1 - sl_pct / 100)
        tp_price = entry_price * (1 + tp_pct / 100)

        self.active_trades[symbol] = {
            "entry_price": entry_price,
            "qty": qty,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "invested": position_size_usdt
        }
        
        self.balance -= position_size_usdt
        self.logger.info(f"📊 [PAPER] ЛОНГ {symbol} | Вход: {entry_price} | SL: {sl_price:.4f} | TP: {tp_price:.4f} | Вложено: {position_size_usdt:.2f} USDT")
        return True

    def update_market_price(self, symbol: str, current_price: float):
        """Сканер присылает сюда новые цены. Проверяем, не выбило ли сделку."""
        if symbol not in self.active_trades:
            return

        trade = self.active_trades[symbol]
        if current_price <= trade["sl_price"]:
            self._close_trade(symbol, current_price, "STOP LOSS 🔴")
        elif current_price >= trade["tp_price"]:
            self._close_trade(symbol, current_price, "TAKE PROFIT 🟢")

    def _close_trade(self, symbol: str, exit_price: float, reason: str):
        trade = self.active_trades.pop(symbol)
        revenue = trade["qty"] * exit_price
        profit = revenue - trade["invested"]
        
        self.balance += revenue
        self.history.append({"symbol": symbol, "profit": profit, "reason": reason})
        
        self.logger.info(f"{reason} | {symbol} закрыт. Прибыль: {profit:.2f} USDT. Новый баланс: {self.balance:.2f} USDT")