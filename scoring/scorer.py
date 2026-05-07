# -*- coding: utf-8 -*-
import pandas as pd

def calculate_score(df: pd.DataFrame, reasons: list, imbalance: float = 1.0) -> float:
    """Оценка качества сигнала с учетом стакана ордеров."""
    score = 0.0
    last = df.iloc[-1]
    
    score += (len(reasons) * 0.5) 

    if 'ATR20' in df.columns:
        atr_pct = (last['ATR20'] / last['close']) * 100
        if 1.5 < atr_pct < 4.0: score += 1.0  
        elif atr_pct >= 4.0: score += 0.3  

    if 'EMA50' in df.columns:
        if last['close'] > last['EMA50']: score += 1.0
        else: score -= 0.5 

    if 'RSI' in df.columns:
        rsi_val = last['RSI']
        if rsi_val > 75: score -= 1.5 
        elif 40 < rsi_val < 65: score += 0.5 

    # 🔥 АНАЛИЗ СТАКАНА (Order Book)
    if imbalance >= 3.0:
        score += 1.5 # Жесткий перевес покупателей (Плита на покупку)
    elif imbalance >= 1.5:
        score += 0.5 # Локальный перевес покупателей
    elif imbalance < 0.5:
        score -= 1.5 # Огромная плита на продажу (опасно!)

    return round(max(0, score), 2)