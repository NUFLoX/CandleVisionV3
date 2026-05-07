
from __future__ import annotations

import numpy as np
import pandas as pd


def _numeric(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in ["open", "high", "low", "close", "volume", "turnover"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = _numeric(df)
    out["prev_close"] = out["close"].shift(1)
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - out["prev_close"]).abs()
    tr3 = (out["low"] - out["prev_close"]).abs()
    out["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    out["atr_14"] = out["tr"].rolling(14, min_periods=1).mean()
    out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()
    out["return_1"] = out["close"].pct_change()
    out["return_3"] = out["close"].pct_change(3)
    out["range_pct"] = np.where(out["close"] > 0, (out["high"] - out["low"]) / out["close"] * 100.0, 0.0)
    out["body_pct"] = np.where(out["open"] > 0, (out["close"] - out["open"]).abs() / out["open"] * 100.0, 0.0)
    out["close_pos"] = np.where(
        (out["high"] - out["low"]).abs() > 1e-12,
        (out["close"] - out["low"]) / (out["high"] - out["low"]),
        0.5,
    )
    out["turnover_ma_20"] = out["turnover"].rolling(20, min_periods=5).mean()
    out["volume_ma_20"] = out["volume"].rolling(20, min_periods=5).mean()
    out["turnover_ratio"] = np.where(out["turnover_ma_20"] > 0, out["turnover"] / out["turnover_ma_20"], 0.0)
    out["volume_ratio"] = np.where(out["volume_ma_20"] > 0, out["volume"] / out["volume_ma_20"], 0.0)
    out["atr_ratio_20"] = np.where(
        out["atr_14"].rolling(20, min_periods=5).mean() > 0,
        out["atr_14"] / out["atr_14"].rolling(20, min_periods=5).mean(),
        0.0,
    )
    out = out.bfill().fillna(0.0)
    return out.infer_objects(copy=False)


def local_support(df: pd.DataFrame, lookback: int) -> float | None:
    if df.empty:
        return None
    window = df.tail(lookback)
    value = pd.to_numeric(window["low"], errors="coerce").min()
    return float(value) if pd.notna(value) else None


def local_resistance(df: pd.DataFrame, lookback: int) -> float | None:
    if df.empty:
        return None
    window = df.tail(lookback)
    value = pd.to_numeric(window["high"], errors="coerce").max()
    return float(value) if pd.notna(value) else None


def rolling_range_pct(df: pd.DataFrame, lookback: int) -> float:
    if df.empty:
        return 0.0
    window = df.tail(lookback)
    high = pd.to_numeric(window["high"], errors="coerce").max()
    low = pd.to_numeric(window["low"], errors="coerce").min()
    close = pd.to_numeric(window["close"], errors="coerce").iloc[-1]
    if not close or pd.isna(close):
        return 0.0
    return float((high - low) / close * 100.0)
