# -*- coding: utf-8 -*-
import asyncio
import logging
import pandas as pd
from api.market import fetch_ohlcv_bybit_async, fetch_all_usdt_pairs_async

class Scout:
    def __init__(self, executor, ws_stream):
        self.logger = logging.getLogger("CandleVision.Scout")
        self.executor = executor
        self.ws_stream = ws_stream
        self.symbols = []
        self.scan_concurrency = 10 
        # Список монет, заблокированных по IP (ошибка 10024)
        self.blacklist = ["0GUSDT", "SONICUSDT", "PENGUUSDT"]

    def __init__(self, executor, ws_stream, sentiment_agent=None):
        self.logger = logging.getLogger("CandleVision.Scout")
        self.executor = executor
        self.ws_stream = ws_stream
        self.sentiment_agent = sentiment_agent

    async def _load_symbols(self):
        if not self.symbols:
            self.logger.info("📡 Получаем актуальный список всех USDT пар с Bybit...")
            self.symbols = await fetch_all_usdt_pairs_async()
            if self.symbols:
                self.logger.info(f"🪙 Загружено {len(self.symbols)} пар для сканирования.")

    def calculate_score(self, df, regime):
        """Зеркальный расчет баллов для Long и Short."""
        score = 0.0
        if df.empty: return score
            
        last = df.iloc[-1]
        prev_3 = df.iloc[-3] if len(df) > 3 else df.iloc[0]
        
        # Общий признак: Сжатие волатильности (энергия для пробоя)
        if last.get('SQZ_ON', False): 
            score += 1.0

        if regime == "BULL":
            # --- ЛОГИКА LONG ---
            if last.get('close', 0) > last.get('MA50', 0): score += 1.0
            if last.get('MACDh', 0) > 0: score += 0.5
            if last.get('close', 0) > prev_3['close']: score += 1.0
        
        elif regime == "BEAR":
            # --- ЛОГИКА SHORT ---
            if last.get('close', 0) < last.get('MA50', 0): score += 1.0
            if last.get('MACDh', 0) < 0: score += 0.5
            if last.get('close', 0) < prev_3['close']: score += 1.0
            
        return score

    async def analyze_symbol(self, symbol, regime):
        if symbol in self.blacklist:
            return None

        try:
            df = await fetch_ohlcv_bybit_async(symbol, '1m', limit=100)
            if df is None or df.empty: return None

            entry_price = float(df['close'].iloc[-1])
            score = self.calculate_score(df, regime)

            # 1. АНАЛИЗ СТАКАНА (OFI и Стены)
            ob_data = self.ws_stream.get_orderbook(symbol)
            bid_wall, ask_wall = None, None
            
            if ob_data:
                ofi, bid_wall, ask_wall = analyze_l2_orderbook(ob_data)

                sentiment_bonus = 0.0
            if self.sentiment_agent:
                sentiment_bonus = self.sentiment_agent.get_sentiment_bonus(symbol)
                
            if regime == "BULL":
                score += sentiment_bonus # Добавляем баллы за позитивные новости
            elif regime == "BEAR":
                score -= sentiment_bonus
                
                if regime == "BULL":
                    if ofi >= 0.3: score += 1.5  # Накидываем баллы за давление покупателей
                    if ofi < 0.0: return None    # Блокируем лонг, если давят продавцы
                    if ask_wall and 0 < (ask_wall - entry_price) / entry_price < 0.01: 
                        return None              # Отменяем сделку, если сверху стена кита
                        
                elif regime == "BEAR":
                    if ofi <= -0.3: score += 1.5 # Накидываем баллы за давление продавцов
                    if ofi > 0.0: return None    # Блокируем шорт, если давят покупатели
                    if bid_wall and 0 < (entry_price - bid_wall) / entry_price < 0.01: 
                        return None              # Отменяем сделку, если снизу стена кита

            # 2. ФИЛЬТР СИГНАЛА
            if score < 2.5:
                if score >= 1.5:
                    await self.executor.queue.put({'symbol': symbol, 'score': score, 'regime': regime})
                return None

            # 3. ФОРМИРОВАНИЕ СИГНАЛА (с умным Stop-Loss)
            side = "Sell" if regime == "BEAR" else "Buy"
            atr = df.iloc[-1].get('ATRr_14', entry_price * 0.005)

            # Прячем стоп за стену кита, если она есть
            if side == "Buy":
                sl = bid_wall * 0.999 if bid_wall else entry_price - (atr * 1.5)
                tp = entry_price + (atr * 2.0)
            else:
                sl = ask_wall * 1.001 if ask_wall else entry_price + (atr * 1.5)
                tp = entry_price - (atr * 2.0)

            signal_data = {
                'symbol': symbol,
                'side': side,
                'score': score,
                'entry_price': entry_price,
                'sl': sl,
                'tp': tp,
                'timeframe': '1m'
            }
            
            self.logger.info(f"🚀 {side.upper()} ПОДТВЕРЖДЕН: {symbol} | Score: {score}")
            await self.executor.queue.put(signal_data)
            return signal_data

        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа {symbol}: {e}")

    async def run_full_market_scan_async(self, regime):
        await self._load_symbols()
        if not self.symbols: return
        self.logger.info(f"🔄 Сканирование ({len(self.symbols)} пар) | Тактика: {regime}")
        
        for i in range(0, len(self.symbols), self.scan_concurrency):
            chunk = self.symbols[i:i + self.scan_concurrency]
            tasks = [self.analyze_symbol(sym, regime) for sym in chunk]
            await asyncio.gather(*tasks)

    async def recheck_watchlist_async(self, targets):
        if not targets: return
        # Перепроверка в зависимости от текущего режима
        tasks = [self.analyze_symbol(t['symbol'], t['regime']) for t in targets]
        await asyncio.gather(*tasks)