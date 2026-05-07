# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Any

# Константы вынесены сюда для автономности модуля. 
# Позже мы перенесем их в глобальный config/settings.py
RISK_MAX_SL_PCT = 10.0
RISK_MIN_TP_PCT = 15.0
RISK_MIN_RR     = 1.20
SR_LOOKBACK      = 120
SR_PIVOT_LEFT    = 3
SR_PIVOT_RIGHT   = 3
SR_PROX_ATR_MULT = 0.5

def _pivots_levels(df: pd.DataFrame, lookback=SR_LOOKBACK, left=SR_PIVOT_LEFT, right=SR_PIVOT_RIGHT):
    """Поиск фрактальных уровней поддержки и сопротивления."""
    n = min(len(df), lookback)
    lows = df["low"].values[-n:]
    highs = df["high"].values[-n:]
    sup, res = [], []
    
    for i in range(left, n - right):
        win_low = lows[i-left:i+right+1]
        win_high = highs[i-left:i+right+1]
        if lows[i] == np.min(win_low):
            sup.append(float(lows[i]))
        if highs[i] == np.max(win_high):
            res.append(float(highs[i]))
            
    def _dedup(levels, tol_pct=0.001):
        levels = sorted(levels)
        out = []
        for lv in levels:
            if not out or abs(lv - out[-1]) / max(1e-9, out[-1]) > tol_pct:
                out.append(lv)
        return out
        
    return _dedup(sup), _dedup(res)

def _nearest_sr(entry_price: float, supports: List[float], resistances: List[float]) -> Tuple[float, float]:
    """Находит ближайшую поддержку (снизу) и сопротивление (сверху)."""
    sup_below = [s for s in supports if s <= entry_price]
    res_above = [r for r in resistances if r >= entry_price]
    nearest_sup = max(sup_below) if sup_below else None
    nearest_res = min(res_above) if res_above else None
    return nearest_sup, nearest_res

def assess_rr(df: pd.DataFrame, entry_price: float) -> Dict[str, Any]:
    """
    Главный гейткипер. Оценивает Risk/Reward. 
    Возвращает словарь с полем 'ok': True/False.
    """
    supports, resistances = _pivots_levels(df)
    atr = float(df["ATR14"].iloc[-1]) if "ATR14" in df.columns and not pd.isna(df["ATR14"].iloc[-1]) else 0.0

    sup, res = _nearest_sr(float(entry_price), supports, resistances)
    if sup is None or res is None:
        return {"ok": False, "sl_pct": None, "tp_pct": None, "rr": None, "why": "no_support_or_resistance"}

    sl_pct = (entry_price - sup) / entry_price * 100.0
    tp_pct = (res - entry_price) / entry_price * 100.0
    rr = (tp_pct / max(sl_pct, 1e-9)) if sl_pct is not None and sl_pct > 0 else None

    ok = True
    why = []
    
    if sl_pct is None or sl_pct > RISK_MAX_SL_PCT:
        ok = False; why.append("sl_too_large")
    if tp_pct is None or tp_pct < RISK_MIN_TP_PCT:
        ok = False; why.append("tp_too_small")
    if rr is None or rr < RISK_MIN_RR:
        ok = False; why.append("rr_too_small")

    # Снайперский фильтр: вход только от уровней поддержки
    if atr > 0 and (entry_price - (sup or entry_price)) > SR_PROX_ATR_MULT * atr:
        ok = False; why.append("far_from_support")

    return {
        "ok": ok,
        "support": sup, "resistance": res,
        "sl_pct": sl_pct, "tp_pct": tp_pct, "rr": rr,
        "why": ",".join(why) if why else "ok"
    }