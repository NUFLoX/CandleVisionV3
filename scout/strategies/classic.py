# -*- coding: utf-8 -*-
from typing import List, Tuple
import numpy as np
import pandas as pd

# Веса из твоего оригинального бота
WEIGHTS_PRIMARY = {
    "RSI↑30": 1.5,
    "RSI<30": 1.0,
    "VolSpike": 1.5,
    "BullEngulf": 1.0,
    "Hammer": 1.0,
    "NearSupport": 1.5,
    "TrendUp": 1.0,
}
WEIGHTS_SECONDARY = {
    "MA7↗MA20": 0.5,
    "MA7↗MA50": 0.5,
    "MACDh↗0": 0.5,
}

THRESHOLD = 2.5
VOL_SPIKE_K = 1.3

def _cross_up(fast_now: float, fast_prev: float, slow_now: float, slow_prev: float) -> bool:
    return fast_now > slow_now and fast_prev <= slow_prev

def _is_bullish_engulf(last, prev) -> bool:
    return (prev["close"] < prev["open"] and last["close"] > last["open"] and
            last["open"] <= prev["close"] and last["close"] >= prev["open"])

def _is_hammer(last) -> bool:
    o, h, l, c = float(last["open"]), float(last["high"]), float(last["low"]), float(last["close"])
    body = max(abs(c - o), 1e-9)
    return ((min(o, c) - l) >= 2.0 * body) and ((h - max(o, c)) <= 0.5 * body) and (c >= o)

def strategy_classic(df: pd.DataFrame, orderbook=None) -> Tuple[bool, List[str]]:
    """
    Твоя оригинальная "Снайперская" стратегия.
    Требует: RSI, VOLUME, MA7, MA20, MA50, MACDh.
    """
    if len(df) < 60:
        return False, ["few-bars"]

    last = df.iloc[-1]
    prev = df.iloc[-2]
    primary, secondary = [], []

    if last.get("MA20", 0) > last.get("MA50", 0):
        primary.append("TrendUp")

    if prev.get("RSI", 50) < 30 <= last.get("RSI", 50):
        primary.append("RSI↑30")
    elif last.get("RSI", 50) < 30:
        primary.append("RSI<30")

    vol_ma20 = df["VOLUME"].tail(20).mean()
    if vol_ma20 > 0 and (last["VOLUME"] / vol_ma20) > VOL_SPIKE_K and last["close"] > last["open"]:
        primary.append("VolSpike")

    if _is_bullish_engulf(last, prev): primary.append("BullEngulf")
    if _is_hammer(last): primary.append("Hammer")

    if _cross_up(last.get("MA7", 0), prev.get("MA7", 0), last.get("MA20", 0), prev.get("MA20", 0)):
        secondary.append("MA7↗MA20")
    if prev.get("MACDh", 0) <= 0 < last.get("MACDh", 0):
        secondary.append("MACDh↗0")

    score = sum(WEIGHTS_PRIMARY.get(k, 0) for k in primary) + sum(WEIGHTS_SECONDARY.get(k, 0) for k in secondary)
    
    is_signal = (len(primary) >= 1) and (score >= THRESHOLD)
    reasons = [f"score={score:.1f}/{THRESHOLD}"] + primary + secondary

    return is_signal, reasons