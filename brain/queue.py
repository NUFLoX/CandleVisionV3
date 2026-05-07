# -*- coding: utf-8 -*-
import queue
import logging

# Локальная очередь сигналов
signal_queue = queue.Queue()

def push_signal(symbol: str, timeframe: str, price: float, df, reasons: list):
    """Сканер использует эту функцию, чтобы передать сигнал."""
    signal = {
        "symbol": symbol,
        "timeframe": timeframe,
        "entry_price": price,
        "df": df,
        "reasons": reasons
    }
    signal_queue.put(signal)
    logging.info(f"📥 [ОЧЕРЕДЬ] Сигнал {symbol} [{timeframe}] принят от Сканера.")

def get_signal():
    """Экзекутор использует эту функцию, чтобы забрать сигнал на проверку."""
    if not signal_queue.empty():
        return signal_queue.get()
    return None
def push_price_update(symbol: str, price: float):
    """Отправляет текущую цену Экзекутору для проверки Стоп-лоссов."""
    signal_queue.put({"type": "price_update", "symbol": symbol, "price": price})