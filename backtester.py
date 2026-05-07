# -*- coding: utf-8 -*-
import requests
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class Backtester:
    def __init__(self):
        # Берем топ-5 самых волатильных и ликвидных на сегодня пар
        self.symbols = ["SOLUSDT", "BTCUSDT", "ETHUSDT", "AVAXUSDT", "NEARUSDT"]
        self.interval = "1"
        self.days = 5

    def fetch_history(self, symbol):
        print(f"📥 Скачиваем историю для {symbol}...")
        end_time = int(time.time() * 1000)
        start_time = int((datetime.now() - timedelta(days=self.days)).timestamp() * 1000)
        all_klines = []
        current_start = start_time
        
        while current_start < end_time:
            url = "https://api.bybit.com/v5/market/kline"
            params = {"category": "linear", "symbol": symbol, "interval": self.interval, "start": current_start, "limit": 1000}
            res = requests.get(url, params=params).json()
            if res.get("retCode") != 0 or not res["result"]["list"]: break
            klines = res["result"]["list"]
            all_klines.extend(klines)
            current_start = int(klines[0][0]) + 60000 
            time.sleep(0.05)
        
        df = pd.DataFrame(all_klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
        df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
        for col in ['open', 'high', 'low', 'close', 'volume']: df[col] = df[col].astype(float)
        return df.sort_values('timestamp').reset_index(drop=True)

    def apply_indicators(self, df):
        # Технический фундамент
        df['SMA_50'] = df['close'].rolling(window=50).mean()
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['MACDh'] = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
        
        # ATR для риск-менеджмента
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['ATR'] = tr.rolling(window=14).mean()

        # Squeeze Momentum
        sma20 = df['close'].rolling(window=20).mean()
        std20 = df['close'].rolling(window=20).std()
        df['sqz_on'] = (sma20 - (2 * std20)) > (sma20 - (1.5 * tr.rolling(window=20).mean()))
        
        # ПРОКСИ ДЛЯ CVD: Всплеск объема (объем свечи > 1.5x от среднего за 20 мин)
        df['vol_ma'] = df['volume'].rolling(window=20).mean()
        df['vol_confirm'] = df['volume'] > (df['vol_ma'] * 1.5)
        
        df.fillna(0, inplace=True)
        return df

    def run_simulation(self, df, symbol):
        in_position = False
        entry_price = sl_price = tp_price = 0
        trades = []
        
        for i in range(50, len(df)):
            row = df.iloc[i]
            prev = df.iloc[i-1]
            
            if in_position:
                if row['low'] <= sl_price:
                    trades.append({'res': 'LOSS', 'pnl': ((sl_price - entry_price) / entry_price) * 100})
                    in_position = False
                elif row['high'] >= tp_price:
                    trades.append({'res': 'WIN', 'pnl': ((tp_price - entry_price) / entry_price) * 100})
                    in_position = False
                continue

            # ЛОГИКА ВХОДА (Индикаторы + Фильтр объема как замена CVD)
            score = 0
            if prev['sqz_on']: score += 1.0
            if prev['MACDh'] > 0: score += 0.5
            if prev['close'] > prev['SMA_50']: score += 0.5
            if prev['close'] > df.iloc[i-3]['close']: score += 0.5

            # Входим только если Score высокий И есть подтверждение объема (наш "CVD" на истории)
            if score >= 2.5 and prev['vol_confirm']:
                in_position = True
                entry_price = row['open']
                atr = prev['ATR'] if prev['ATR'] > 0 else entry_price * 0.005
                sl_price = entry_price - (atr * 1.5)
                tp_price = entry_price + (atr * 2.0)

        wins = len([t for t in trades if t['res'] == 'WIN'])
        total = len(trades)
        winrate = (wins / total * 100) if total > 0 else 0
        pnl = sum([t['pnl'] for t in trades])

        print(f"\n📊 ОТЧЕТ ПО {symbol} (с фильтром объема):")
        print(f"Сигналов: {total} | Winrate: {winrate:.1f}% | Чистый PnL: {pnl:.2f}%")

if __name__ == "__main__":
    tester = Backtester()
    for sym in tester.symbols:
        df = tester.fetch_history(sym)
        df = tester.apply_indicators(df)
        tester.run_simulation(df, sym)