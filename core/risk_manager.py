# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Any

# Conservative defaults for intraday crypto signals. Values are percentages.
RISK_MAX_SL_PCT = 6.0
RISK_MIN_TP_PCT = 0.8
RISK_MIN_RR = 1.20
SR_LOOKBACK = 120
SR_PIVOT_LEFT = 3
SR_PIVOT_RIGHT = 3
SR_PROX_ATR_MULT = 1.5
ATR_STOP_MULT = 1.25
FALLBACK_TP_RR = 2.0


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


def _nearest_sr(entry_price: float, supports: List[float], resistances: List[float]) -> Tuple[float | None, float | None]:
    """Находит ближайшую поддержку (снизу) и сопротивление (сверху)."""
    sup_below = [s for s in supports if s < entry_price]
    res_above = [r for r in resistances if r > entry_price]
    nearest_sup = max(sup_below) if sup_below else None
    nearest_res = min(res_above) if res_above else None
    return nearest_sup, nearest_res


def _last_atr(df: pd.DataFrame, entry_price: float) -> float:
    for column in ("ATR20", "ATR14", "atr_14"):
        if column in df.columns and not pd.isna(df[column].iloc[-1]):
            value = float(df[column].iloc[-1])
            if value > 0:
                return value

    if len(df) < 2:
        return entry_price * 0.01
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = float(tr.tail(14).mean())
    return atr if atr > 0 and not pd.isna(atr) else entry_price * 0.01


def assess_rr(df: pd.DataFrame, entry_price: float) -> Dict[str, Any]:
    """
    Оценивает Risk/Reward и возвращает готовые SL/TP для контракта Scout -> Executor.
    """
    entry_price = float(entry_price)
    if df.empty or entry_price <= 0:
        return {"ok": False, "why": "bad_input", "sl": None, "tp": None, "rr": None}

    supports, resistances = _pivots_levels(df)
    sup, res = _nearest_sr(entry_price, supports, resistances)
    atr = _last_atr(df, entry_price)

    if sup is None:
        sup = float(pd.to_numeric(df["low"], errors="coerce").tail(20).min())
    if not sup or pd.isna(sup) or sup >= entry_price:
        return {"ok": False, "why": "no_valid_support", "sl": None, "tp": None, "rr": None}

    structural_stop = sup - atr * 0.25
    atr_stop = entry_price - atr * ATR_STOP_MULT
    sl = min(structural_stop, atr_stop)
    if sl <= 0 or sl >= entry_price:
        return {"ok": False, "why": "bad_stop", "sl": sl, "tp": None, "rr": None}

    risk = entry_price - sl
    if res is None or res <= entry_price:
        res = entry_price + risk * FALLBACK_TP_RR
    tp = max(res, entry_price + risk * FALLBACK_TP_RR)

    sl_pct = risk / entry_price * 100.0
    tp_pct = (tp - entry_price) / entry_price * 100.0
    rr = (tp - entry_price) / max(risk, 1e-12)

    why = []
    if sl_pct > RISK_MAX_SL_PCT:
        why.append("sl_too_large")
    if tp_pct < RISK_MIN_TP_PCT:
        why.append("tp_too_small")
    if rr < RISK_MIN_RR:
        why.append("rr_too_small")
    if atr > 0 and (entry_price - sup) > SR_PROX_ATR_MULT * atr:
        why.append("far_from_support")

    return {
        "ok": not why,
        "entry": entry_price,
        "sl": float(sl),
        "tp": float(tp),
        "support": float(sup),
        "resistance": float(res),
        "sl_pct": sl_pct,
        "tp_pct": tp_pct,
        "rr": rr,
        "why": ",".join(why) if why else "ok",
    }
