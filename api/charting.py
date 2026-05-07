# -*- coding: utf-8 -*-
import pandas as pd
import os

# ДОБАВЬ ЭТИ ДВЕ СТРОКИ:
import matplotlib
matplotlib.use('Agg') # Отключает попытки открыть графическое окно

import mplfinance as mpf # Этот импорт должен быть НИЖЕ

def generate_setup_chart(df: pd.DataFrame, symbol: str, support: float, resistance: float, filename="setup_chart.png") -> str:
    """Генерирует свечной график с уровнями поддержки/сопротивления."""
    try:
        plot_df = df.tail(60).copy()
        
        # Исправляем время для графика
        plot_df['time'] = pd.to_datetime(plot_df['time'], unit='ms')
        plot_df.set_index('time', inplace=True)

        # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: приводим колонки к формату, который требует mplfinance
        # Мы переименовываем наши 'open', 'high', 'low', 'close', 'VOLUME' в нужный вид
        plot_df.rename(columns={
            'open': 'Open', 'high': 'High', 'low': 'Low', 
            'close': 'Close', 'VOLUME': 'Volume'
        }, inplace=True)

        # Настраиваем уровни
        hlines = dict(hlines=[support, resistance], colors=['g', 'r'], linestyle='-.', linewidths=1.5)

        # Рисуем
        mpf.plot(plot_df, type='candle', style='charles', title=f"{symbol} Setup",
                 hlines=hlines, volume=True, savefig=filename)
        
        return filename
    except Exception as e:
        import logging
        logging.error(f"❌ Ошибка генерации графика для {symbol}: {e}")
        return None