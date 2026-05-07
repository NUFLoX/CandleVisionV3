from __future__ import annotations

import numpy as np
import pandas as pd


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    numeric_cols = ["open", "high", "low", "close", "volume", "turnover"]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    close_safe = out["close"].replace(0.0, np.nan)
    range_safe = (out["high"] - out["low"]).replace(0.0, np.nan)

    out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()
    out["volume_ma_20"] = out["turnover"].rolling(20, min_periods=1).mean()

    high_low = out["high"] - out["low"]
    high_close = (out["high"] - out["close"].shift()).abs()
    low_close = (out["low"] - out["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

    out["atr_14"] = tr.rolling(14, min_periods=1).mean()
    out["range_pct"] = ((out["high"] - out["low"]) / close_safe) * 100.0
    out["close_pos"] = (out["close"] - out["low"]) / range_safe
    out["return_3"] = out["close"].pct_change(3)

    out = out.bfill().fillna(0.0)
    return out


def local_support(df: pd.DataFrame, lookback: int) -> float | None:
    if df.empty:
        return None
    window = df.tail(lookback)
    if window.empty:
        return None
    return float(window["low"].min())


def local_resistance(df: pd.DataFrame, lookback: int) -> float | None:
    if df.empty:
        return None
    window = df.tail(lookback)
    if window.empty:
        return None
    return float(window["high"].max())
