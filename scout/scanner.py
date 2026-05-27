# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pandas as pd

from api.market import fetch_ohlcv_bybit_async
from config.settings import SCOUT_SCAN_TIMEFRAMES
from core.risk_manager import assess_rr
from scoring.scorer import calculate_score


class Scout:
    def __init__(
        self,
        queue,
        strategies=None,
        ws_stream=None,
        tape_agent=None,
        dashboard_client: Any | None = None,
    ):
        self.logger = logging.getLogger("CandleVision.Scout")
        self.queue = queue
        self.strategies = strategies or []
        self.symbols = []
        self.ws_stream = ws_stream
        self.tape_agent = tape_agent
        self.dashboard = dashboard_client

    def load_symbols(self, symbols_list: list):
        self.symbols = symbols_list
        self.logger.info(f"🪙 Загружено {len(self.symbols)} пар для сканирования.")

    async def _post_watchlist_safe(
        self,
        symbol: str,
        timeframe: str,
        score: float,
        reason: str,
    ) -> None:
        if not self.dashboard:
            return

        try:
            await self.dashboard.post_watchlist(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "score": score,
                    "reason": reason,
                    "status": "WATCHING",
                }
            )
        except TypeError:
            # Fallback на старую сигнатуру, если dashboard client её использует.
            await self.dashboard.post_watchlist(
                symbol,
                timeframe=timeframe,
                score=score,
                reason=reason,
            )
        except Exception as exc:
            self.logger.debug(f"Dashboard watchlist ingest failed for {symbol}: {exc}")

    async def _post_heartbeat_safe(self, mode: str, meta: dict[str, Any]) -> None:
        if not self.dashboard:
            return

        try:
            await self.dashboard.post_heartbeat(
                "scanner",
                status="OK",
                message=f"Scout {mode}",
                meta=meta,
            )
        except TypeError:
            # Fallback на старую сигнатуру.
            await self.dashboard.post_heartbeat("scanner", meta=meta)
        except Exception as exc:
            self.logger.debug(f"Dashboard heartbeat failed: {exc}")

    async def run_scan_async(self, symbol: str, tf: str):
        df = await fetch_ohlcv_bybit_async(symbol, tf, 100)

        if df.empty or len(df) < 50:
            return

        reasons = []
        found_any = False

        for strategy in self.strategies:
            result, msg = strategy(df)

            if result:
                found_any = True

                if isinstance(msg, list):
                    reasons.extend(msg)
                elif msg:
                    reasons.append(str(msg))

        if not found_any:
            return

        imbalance = 0.0

        if self.ws_stream:
            await self.ws_stream.subscribe(symbol)
            imbalance = self.ws_stream.get_imbalance(symbol)

        risk = assess_rr(df, float(df["close"].iloc[-1]))

        if not risk.get("ok"):
            self.logger.debug(f"🧯 {symbol}: RR-фильтр отклонил сигнал ({risk.get('why')})")
            return

        score = calculate_score(df, reasons, imbalance)

        whale_bonus = 0.0

        if getattr(self, "tape_agent", None):
            whale_bonus = self.tape_agent.get_whale_bonus(symbol)

        score += whale_bonus

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
                "reasons": reasons
                + [
                    f"RR={risk['rr']:.2f}",
                    f"SL={risk['sl_pct']:.2f}%",
                    f"TP={risk['tp_pct']:.2f}%",
                ],
                "imbalance": imbalance,
                "df": df.tail(100),
            }

            await self.queue.put(signal_data)

            await self._post_watchlist_safe(
                symbol=symbol,
                timeframe=tf,
                score=score,
                reason="; ".join(reasons[:4]) if reasons else "scout_signal",
            )

    async def recheck_watchlist_async(self, watchlist_symbols: list):
        """Метод 'Спецназа': быстрая проверка избранных монет."""
        if not watchlist_symbols:
            return

        await self._post_heartbeat_safe(
            "watchlist",
            {
                "runner": "scout",
                "mode": "watchlist",
                "symbols": len(watchlist_symbols),
            },
        )

        self.logger.info(f"🔎 Перепроверка Watchlist ({len(watchlist_symbols)} пар)...")

        for symbol in watchlist_symbols:
            for tf in SCOUT_SCAN_TIMEFRAMES:
                await self.run_scan_async(symbol, tf)
                await asyncio.sleep(0.3)

    async def run_full_market_scan_async(self, regime: str = "FLAT"):
        await self._post_heartbeat_safe(
            "full",
            {
                "runner": "scout",
                "mode": "full",
                "symbols": len(self.symbols),
                "regime": regime,
            },
        )

        self.logger.info(f"🔄 Сканирование ({len(self.symbols)} пар) | Тактика: {regime}")

        for symbol in self.symbols:
            for tf in SCOUT_SCAN_TIMEFRAMES:
                await self.run_scan_async(symbol, tf)
                await asyncio.sleep(0.2)

        self.logger.info("✅ Цикл сканирования завершен.")
