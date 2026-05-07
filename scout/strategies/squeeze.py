# -*- coding: utf-8 -*-
from typing import List, Tuple
import pandas as pd
from core.indicators import calc_point_of_control

def strategy_squeeze(df: pd.DataFrame, orderbook=None) -> Tuple[bool, List[str]]:
    """
    Стратегия Squeeze Momentum + Volume Profile.
    Ищет монеты, которые находятся в стадии сжатия (накопления) и готовы к выстрелу вверх.
    """
    if len(df) < 50 or 'SQZ_ON' not in df.columns:
        return False, ["no_data"]

    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    reasons = []
    
    # 1. Проверяем, есть ли сейчас "Сжатие" (Пружина сжимается)
    is_squeezing = last['SQZ_ON'] == True
    
    # 2. Проверяем, начал ли расти Моментум (Пружина начинает разжиматься вверх)
    momentum_rising = (last['MOMENTUM'] > prev['MOMENTUM']) and (last['MOMENTUM'] > 0)
    
    # 3. Рассчитываем POC (уровень максимального объема за 50 свечей)
    poc_price = calc_point_of_control(df, lookback=50)
    current_price = float(last['close'])
    
    # Мы хотим покупать, если цена находится ВЫШЕ или НА уровне POC (POC выступает поддержкой)
    near_poc = current_price >= (poc_price * 0.99)
    
    if is_squeezing and momentum_rising and near_poc:
        reasons.append("Squeeze_ON")
        reasons.append("Momentum_UP")
        reasons.append(f"Above_POC({poc_price:.4f})")
        return True, reasons

    return False, reasons