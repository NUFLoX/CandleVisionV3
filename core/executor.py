# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import time
import math

from agents.notifier import TelegramNotifier
from api.telegram import TelegramReporter
from api.charting import generate_setup_chart
from api.bybit_client import BybitClient

def calculate_position_size(balance, risk, entry, sl):
    """Рассчитывает объем позиции."""
    distance = abs(entry - sl)
    if distance == 0: return 0
    return round((balance * (risk / 100)) / distance, 4)

class Executor:
    def __init__(self, db, initial_balance=1000):
        self.logger = logging.getLogger("CandleVision.Executor")
        self.db = db
        self.risk_percent = 1.0
        self.queue = None

        self.watchlist = set()
        self.max_watchlist_size = 50
        self.max_daily_loss = -50.0
        self.current_daily_pnl = 0.0

        # ========== ИНСТИТУЦИОНАЛЬНЫЕ ЛИМИТЫ ==========
        self.max_positions = 5               # Максимум 5 одновременных позиций
        self.start_time = time.time()        # Время запуска
        self.warmup_seconds = 180            # Прогрев 3 минуты
        self.tp1_ratio = 0.5                 # Закрываем 50% на первом тейке

        # --- НАШ НОВЫЙ TELEGRAM NOTIFIER ---
        BOT_TOKEN = "8484244730:AAFTNM_XjI6nFGtZoDcOKiwzb83p609ks5A" 
        CHAT_ID = "395353141"
        self.notifier = TelegramNotifier(bot_token=BOT_TOKEN, chat_id=CHAT_ID)
        # -----------------------------------

        self.tg = TelegramReporter()
        self.exchange = BybitClient(testnet=True)

        self.active_trades = self.db.load_open_trades()
        if self.active_trades:
            self.logger.info(f"💾 Восстановлено {len(self.active_trades)} открытых позиций.")

        real_balance = self._get_real_balance()
        self.balance = real_balance if real_balance is not None else initial_balance

    def _get_real_balance(self):
        """Запрашивает реальный баланс USDT на бирже."""
        if not self.exchange.session:
            return None
        try:
            response = self.exchange.session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
            if response.get("retCode") == 0:
                coins = response.get("result", {}).get("list", [{}])[0].get("coin", [])
                for coin_data in coins:
                    if coin_data.get("coin") == "USDT":
                        balance = float(coin_data.get("walletBalance", 0))
                        self.logger.info(f"💰 Реальный баланс Bybit: {balance:.2f} USDT")
                        return balance
        except Exception as e:
            self.logger.error(f"❌ Ошибка запроса баланса: {e}")

        self.logger.warning("Используется виртуальный баланс: 1000 USDT (из-за ошибки API)")
        return None

    async def process_signal_async(self, signal_data):
        elapsed = time.time() - self.start_time
        if elapsed < self.warmup_seconds:
            remaining = int(self.warmup_seconds - elapsed)
            if int(elapsed) % 30 == 0 and elapsed > 0:
                self.logger.info(f"⏳ Прогрев данных... осталось {remaining}с")
            return False

        if self.current_daily_pnl <= self.max_daily_loss:
            self.logger.warning("⛔️ Достигнут лимит убытков на день!")
            return False

        active_count = len([t for t in self.active_trades if t['status'] == 'open'])
        if active_count >= self.max_positions:
            self.logger.debug(f"🛑 Максимум позиций ({self.max_positions}). Ждём свободный слот.")
            return False

        # Читаем ТОЛЬКО базовые данные (они есть в любом сигнале)
        symbol = signal_data['symbol']
        score = signal_data['score']

        if any(t['symbol'] == symbol and t['status'] == 'open' for t in self.active_trades):
            return False

        # СНАЧАЛА ПРОВЕРЯЕМ WATCHLIST
        if score < 2.5:
            if symbol not in self.watchlist:
                if len(self.watchlist) >= self.max_watchlist_size:
                    self.watchlist.pop()
                self.logger.info(f"⏳ {symbol} в Watchlist (Score: {score})")
                self.watchlist.add(symbol)
            return False

        # ЕСЛИ СИГНАЛ ПРОШЕЛ (Score >= 2.5) — ДОСТАЕМ ОСТАЛЬНЫЕ ДАННЫЕ
        df = signal_data.get('df')
        entry = signal_data.get('entry_price')
        side = signal_data.get('side', 'Buy')
        sl = signal_data.get('sl')
        tp = signal_data.get('tp')

        # Защита от пустых данных: если ключей нет, просто игнорим сигнал
        if not sl or not tp or df is None or entry is None:
            return False

        if symbol in self.watchlist:
            self.logger.info(f"🎯 СИГНАЛ ДОЗРЕЛ: {symbol}")
            self.watchlist.remove(symbol)

        size = calculate_position_size(self.balance, self.risk_percent, entry, sl)
        if size <= 0: return False

        # Рассчитываем TP1 
        tp1_price = round(entry + (tp - entry) * 0.5, 7 if entry < 0.01 else 4)
        tp1_qty = round(size * self.tp1_ratio, 4)

        # === ВЫТАСКИВАЕМ ПРИЧИНЫ ДЛЯ САМООБУЧЕНИЯ ===
        reasons_list = signal_data.get('reasons', ['Technical Analysis'])
        reasons_str = ", ".join(reasons_list) if isinstance(reasons_list, list) else str(reasons_list)
        # ============================================

        trade = {
            "symbol": symbol, "side": side, "entry": entry, "sl": sl, "tp": tp,
            "tp1_price": tp1_price, "tp1_qty": tp1_qty,
            "size": size, "remaining_size": size,
            "status": "open", "pnl_pct": 0.0, "timeframe": signal_data.get('timeframe', '1m'),
            "reasons": reasons_str # <--- ПЕРЕДАЕМ ИХ В БАЗУ ДАННЫХ
        }

        trade_id = self.db.add_trade(trade)
        trade["id"] = trade_id
        self.active_trades.append(trade)

        log_decimals = 7 if entry < 0.01 else 4
        risk_amount = size * abs(entry - sl)
        sl_pct = abs(entry - sl) / entry * 100
        tp_pct = abs(tp - entry) / entry * 100

        self.logger.info(
            f"🚀 ВХОД ({side.upper()}): {trade['symbol']} | Score: {score:.2f} | "
            f"Entry: {trade['entry']:.{log_decimals}f} | "
            f"SL: {trade['sl']:.{log_decimals}f} ({sl_pct:.1f}%) | "
            f"TP: {trade['tp']:.{log_decimals}f} ({tp_pct:.1f}%) | "
            f"Size: {trade['size']:.4f} | Risk: {risk_amount:.2f}$"
        )

        trade_success = False
        if self.exchange.session:
            trade_success = self._place_limit_order(
                symbol=symbol, side=side, qty=size, entry_price=entry, sl_price=sl, tp_price=tp
            )
            if not trade_success:
                self.logger.error(f"❌ Ордер {symbol} не исполнен. Удаляем из БД.")
                self.db.cursor.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
                self.db.conn.commit()
                self.active_trades.remove(trade)

        asyncio.create_task(self._send_execution_report(trade, score, df))
        return trade_success

    def _place_limit_order(self, symbol, side, qty, entry_price, sl_price, tp_price):
        try:
            if entry_price < 10: formatted_qty = int(qty)
            elif entry_price < 1000: formatted_qty = round(qty, 2)
            else: formatted_qty = round(qty, 3)

            # Для Short цена лимитки должна быть чуть ниже текущей, для Long - чуть выше
            limit_offset = 0.999 if side == "Sell" else 1.001
            limit_price = round(entry_price * limit_offset, 7 if entry_price < 0.01 else 4)

            order_response = self.exchange.session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=str(formatted_qty),
                price=str(limit_price),
                stopLoss=str(sl_price),
                takeProfit=str(tp_price),
                positionIdx=0,
                timeInForce="GTC"
            )
            order_id = order_response.get('result', {}).get('orderId', 'N/A')
            self.logger.info(f"📝 Ордер {symbol} ({side}) размещён (ID: {order_id})")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка ордера {symbol}: {e}")
            return False

    async def update_positions(self, price_updates: dict):
        for trade in self.active_trades:
            if trade['status'] != 'open': continue
            symbol = trade['symbol']
            if symbol not in price_updates: continue

            current_price = price_updates[symbol]
            side = trade.get('side', 'Buy')

            # ========== ПРОВЕРКА TP1 ==========
            hit_tp1 = False
            if side == "Sell" and current_price <= trade['tp1_price']: hit_tp1 = True
            if side == "Buy" and current_price >= trade['tp1_price']: hit_tp1 = True

            if hit_tp1 and trade['remaining_size'] > trade['size'] * 0.5:
                close_qty = trade['tp1_qty']
                if trade['entry'] < 10: close_qty_fmt = int(close_qty)
                elif trade['entry'] < 1000: close_qty_fmt = round(close_qty, 2)
                else: close_qty_fmt = round(close_qty, 3)

                try:
                    if self.exchange.session:
                        # Обратный ордер для закрытия
                        close_side = "Buy" if side == "Sell" else "Sell"
                        self.exchange.session.place_order(
                            category="linear",
                            symbol=symbol,
                            side=close_side,
                            orderType="Market",
                            qty=str(close_qty_fmt),
                            reduceOnly=True
                        )
                    self.logger.info(f"💰 {symbol}: TP1 исполнен! Закрыто {close_qty_fmt} монет.")
                    trade['remaining_size'] = round(trade['remaining_size'] - close_qty, 4)
                except Exception as e:
                    self.logger.error(f"❌ Ошибка исполнения TP1 для {symbol}: {e}")

            # ========== ЛОКАЛЬНАЯ СИНХРОНИЗАЦИЯ (БД) ==========
            is_stop_loss = (side == "Buy" and current_price <= trade['sl']) or (side == "Sell" and current_price >= trade['sl'])
            is_take_profit = (side == "Buy" and current_price >= trade['tp']) or (side == "Sell" and current_price <= trade['tp'])

            if is_stop_loss or is_take_profit:
                pnl_pct = ((current_price - trade['entry']) / trade['entry']) * 100
                if side == "Sell": pnl_pct = -pnl_pct # Инвертируем процент для шорта
                
                reason = "STOP LOSS" if is_stop_loss else "TAKE PROFIT"
                self._close_position(trade, current_price, reason, pnl_pct)

    def _close_position(self, trade, exit_price, reason, pnl_pct):
        trade['status'] = 'closed'
        trade['pnl_pct'] = pnl_pct
        self.db.update_trade_status(trade['id'], 'closed', pnl_pct)

        # Профит в деньгах
        diff = exit_price - trade['entry']
        if trade.get('side') == "Sell": diff = -diff
        
        profit = diff * trade['remaining_size']
        self.current_daily_pnl += profit
        self.balance += profit

        emoji = "🔴" if reason == "STOP LOSS" else "🟢"
        self.logger.info(f"{emoji} {reason}: {trade['symbol']} закрыт | P&L: {pnl_pct:.2f}% | Баланс: {self.balance:.2f}$")

    async def _send_execution_report(self, trade, score, df):
        try:
            # 1. Формируем красивый текст
            msg = (
                f"🚀 <b>СИГНАЛ: {trade['symbol']}</b>\n"
                f"🎯 <b>Тип:</b> {trade['side']}\n"
                f"💰 <b>Вход:</b> {trade['entry']}\n"
                f"🧠 <b>Балл:</b> {score:.2f}\n"
                f"🛑 <b>SL:</b> {trade['sl']} | 🟢 <b>TP:</b> {trade['tp']}"
            )
            
            # 2. Генерируем график
            filename = f"setup_{trade['symbol']}.png"
            photo_path = await asyncio.to_thread(
                generate_setup_chart, df, trade['symbol'], trade['sl'], trade['tp'], filename
            )
            
            # 3. Отправляем фото + текст через нашего BotFather-бота
            if photo_path and os.path.exists(photo_path):
                await self.notifier.send_photo(photo_path, caption=msg)
                os.remove(photo_path) # Удаляем картинку с компа после отправки
            else:
                await self.notifier.send_message(msg) # Если график не создался, шлем просто текст

        except Exception as e:
            self.logger.error(f"❌ Не удалось отправить отчет с графиком: {e}")