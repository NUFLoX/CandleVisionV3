# -*- coding: utf-8 -*-
from typing import List, Tuple
import pandas as pd

def strategy_pump(df: pd.DataFrame, orderbook=None) -> Tuple[bool, List[str]]:
    """
    Стратегия поиска пампов (Breakout / Pump Sniper).
    Ищет резкий выход из консолидации на аномальном объеме.
    """
    if len(df) < 20:
        return False, ["few-bars"]

    last = df.iloc[-1]
    
    # 1. Проверка на "вспышку" объема (x3 от среднего за 20 свечей)
    vol_ma20 = df["VOLUME"].tail(20).mean()
    vol_spike = (last["VOLUME"] / vol_ma20) > 3.0 if vol_ma20 > 0 else False
    
    # 2. Проверка на сильную зеленую свечу (закрытие почти на самом хае)
    o, h, l, c = last["open"], last["high"], last["low"], last["close"]
    body = abs(c - o)
    is_strong_green = (c > o) and (body / max((h - l), 1e-9) > 0.7) # Тело свечи занимает 70% всей длины
    
    # 3. Пробой локального максимума (за 20 свечей)
    local_high = df["high"].tail(20).max()
    is_breakout = (c >= local_high * 0.99) # Цена закрылась у самого максимума
    
    reasons = []
    if vol_spike and is_strong_green and is_breakout:
        reasons = ["VolumeBreakout(x3)", "StrongGreen", "LocalHighBreak"]
        return True, reasons

    return False, reasons