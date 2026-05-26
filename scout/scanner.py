# -*- coding: utf-8 -*-
import logging
import asyncio
import pandas as pd
from scoring.scorer import calculate_score
from api.market import fetch_ohlcv_bybit_async
from core.risk_manager import assess_rr
from config.settings import SCOUT_SCAN_TIMEFRAMES

class Scout:
    def __init__(self, queue, strategies=None, ws_stream=None, tape_agent=None):
        self.logger = logging.getLogger("CandleVision.Scout")
        self.queue = queue
        self.strategies = strategies or []
        self.symbols = []
        self.ws_stream = ws_stream
        self.tape_agent = tape_agent  # <--- Добавили агента китов
        self.dashboard = DashboardIngestClient()

    def load_symbols(self, symbols_list: list):
        self.symbols = symbols_list
        self.logger.info(f"🪙 Загружено {len(self.symbols)} пар для сканирования.")

    async def run_scan_async(self, symbol: str, tf: str):
        df = await fetch_ohlcv_bybit_async(symbol, tf, 100)
        if df.empty or len(df) < 50: return

        reasons = []
        found_any = False

        for strategy in self.strategies:
            result, msg = strategy(df) 
            if result:
                found_any = True
                reasons.extend(msg)

        if found_any:
            imbalance = 0.0
            if self.ws_stream:
                await self.ws_stream.subscribe(symbol)
                imbalance = self.ws_stream.get_imbalance(symbol)

            risk = assess_rr(df, float(df['close'].iloc[-1]))
            if not risk.get("ok"):
                self.logger.debug(f"🧯 {symbol}: RR-фильтр отклонил сигнал ({risk.get('why')})")
                return

            score = calculate_score(df, reasons, imbalance)

            # === 1.7. ON-CHAIN FLOW (КИТЫ) ===
            whale_bonus = 0.0
            if getattr(self, 'tape_agent', None):
                whale_bonus = self.tape_agent.get_whale_bonus(symbol)
            
            # Киты покупают - бустим лонг. Киты продают - режем балл!
            score += whale_bonus
            # =================================
            
            if score >= 1.5: 
                signal_data = {
                    "symbol": symbol,
                    "timeframe": tf,
                    "entry_price": risk["entry"],
                    "score": score,
                    "side": "Buy",
                    "sl": risk["sl"],
                    "tp": risk["tp"],
                    "rr": risk["rr"],
                    "reasons": reasons + [f"RR={risk['rr']:.2f}", f"SL={risk['sl_pct']:.2f}%", f"TP={risk['tp_pct']:.2f}%"],
                    "imbalance": imbalance,
                    "df": df.tail(100) 
                }
                await self.queue.put(signal_data) 
                await self.dashboard.post_watchlist(
                    symbol,
                    timeframe=tf,
                    score=score,
                    reason="; ".join(reasons[:4]) if reasons else "scout_signal",
                )

    async def recheck_watchlist_async(self, watchlist_symbols: list):
        """Метод 'Спецназа': быстрая проверка избранных монет."""
        if not watchlist_symbols: return
        await self.dashboard.post_heartbeat("scanner", meta={"runner": "scout", "mode": "watchlist", "symbols": len(watchlist_symbols)})
        self.logger.info(f"🔎 Перепроверка Watchlist ({len(watchlist_symbols)} пар)...")
        for symbol in watchlist_symbols:
            for tf in SCOUT_SCAN_TIMEFRAMES:
                await self.run_scan_async(symbol, tf)
                await asyncio.sleep(0.3) 

    # ДОБАВИЛИ ПАРАМЕТР regime
    async def run_full_market_scan_async(self, regime: str = "FLAT"):
        await self.dashboard.post_heartbeat("scanner", meta={"runner": "scout", "mode": "full", "symbols": len(self.symbols), "regime": regime})
        self.logger.info(f"🔄 Сканирование ({len(self.symbols)} пар) | Тактика: {regime}")
        for symbol in self.symbols:
            # В будущем мы передадим regime внутрь run_scan_async,
            # чтобы стратегии знали, что искать (лонг или шорт).
            for tf in SCOUT_SCAN_TIMEFRAMES:
                await self.run_scan_async(symbol, tf)
                await asyncio.sleep(0.2)
            self.logger.info("✅ Цикл сканирования завершен.")
