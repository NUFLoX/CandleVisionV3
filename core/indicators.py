# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def calc_point_of_control(df: pd.DataFrame, lookback: int = 50) -> float:
    """
    Рассчитывает Point of Control (POC) — ценовой уровень с максимальным проторгованным объемом.
    """
    if len(df) < lookback:
        lookback = len(df)
        
    recent_df = df.tail(lookback).copy()
    
    # Разбиваем диапазон цен на 20 корзин (bins)
    bins = np.linspace(recent_df['low'].min(), recent_df['high'].max(), 20)
    recent_df['price_bin'] = pd.cut(recent_df['close'], bins=bins, include_lowest=True)
    
    # Считаем объем для каждой корзины
    volume_profile = recent_df.groupby('price_bin', observed=False)['VOLUME'].sum()
    
    # Находим корзину с максимальным объемом и берем ее середину
    poc_bin = volume_profile.idxmax()
    poc_price = poc_bin.mid
    
    return float(poc_price)

def add_advanced_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обогащает DataFrame сложными индикаторами: Squeeze Momentum, ATR, базовые MA.
    """
    if len(df) < 20:
        return df

    length = 20
    
    # 1. Базовые скользящие
    df['SMA20'] = df['close'].rolling(window=length).mean()
    df['EMA50'] = df['close'].ewm(span=50, adjust=False).mean()
    
    # 2. Squeeze Momentum (по Джону Картеру)
    # Расчет Bollinger Bands
    df['STD20'] = df['close'].rolling(window=length).std()
    df['BB_UPPER'] = df['SMA20'] + (2.0 * df['STD20'])
    df['BB_LOWER'] = df['SMA20'] - (2.0 * df['STD20'])
    
    # Расчет Keltner Channels
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['ATR20'] = tr.rolling(window=length).mean()
    
    df['KC_UPPER'] = df['SMA20'] + (1.5 * df['ATR20'])
    df['KC_LOWER'] = df['SMA20'] - (1.5 * df['ATR20'])
    
    # Условие Squeeze (Сжатие): Bollinger Bands полностью внутри Keltner Channels
    df['SQZ_ON'] = (df['BB_LOWER'] > df['KC_LOWER']) & (df['BB_UPPER'] < df['KC_UPPER'])
    
    # Упрощенный Momentum (направление выхода из сжатия)
    df['MOMENTUM'] = df['close'] - df['SMA20']

    return df