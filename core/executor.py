# -*- coding: utf-8 -*-
import asyncio
import logging
import os
import time
import math
import uuid
from decimal import Decimal, ROUND_DOWN

from agents.notifier import TelegramNotifier
from api.telegram import TelegramReporter
from api.charting import generate_setup_chart
from api.bybit_client import BybitClient
from config.settings import TOKEN, CHAT_ID, BYBIT_TESTNET, SIGNALS_ONLY, trading_enabled
from dashboard.ingest_client import DashboardIngestClient

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

        # --- TELEGRAM NOTIFIER ---
        self.notifier = TelegramNotifier(bot_token=TOKEN, chat_id=CHAT_ID)
        # -----------------------------------

        self.tg = TelegramReporter()
        self.exchange = BybitClient(testnet=BYBIT_TESTNET)
        self.dashboard = DashboardIngestClient()
        self.signals_only = SIGNALS_ONLY
        self.instrument_rules: dict[str, dict] = {}
        self.order_link_ids: set[str] = set()

        self.active_trades = self.db.load_open_trades()
        if self.active_trades:
            self.logger.info(f"💾 Восстановлено {len(self.active_trades)} открытых позиций.")

        if not self.signals_only:
            self._sync_exchange_state()
        else:
            self.logger.warning("🛡️ SIGNALS_ONLY=true: биржевые ордера жестко отключены в Executor.")

        real_balance = self._get_real_balance() if not self.signals_only else None
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

        active_count = len([t for t in self.active_trades if t.get('status') in {'open', 'pending_order'}])
        if active_count >= self.max_positions:
            self.logger.debug(f"🛑 Максимум позиций ({self.max_positions}). Ждём свободный слот.")
            return False

        # Читаем ТОЛЬКО базовые данные (они есть в любом сигнале)
        symbol = signal_data['symbol']
        score = signal_data['score']

        if self._has_local_active_trade(symbol) or self._has_exchange_duplicate(symbol):
            self.logger.info(f"🔁 {symbol}: активная сделка/ордер уже существует, дубль пропущен.")
            return False

        # СНАЧАЛА ПРОВЕРЯЕМ WATCHLIST
        if score < 2.5:
            if symbol not in self.watchlist:
                if len(self.watchlist) >= self.max_watchlist_size:
                    self.watchlist.pop()
                self.logger.info(f"⏳ {symbol} в Watchlist (Score: {score})")
                self.watchlist.add(symbol)
                asyncio.create_task(
                    self.dashboard.post_watchlist(
                        symbol,
                        timeframe=signal_data.get('timeframe', '1m'),
                        score=float(score),
                        reason=", ".join(signal_data.get('reasons', ['scanner watchlist'])) if isinstance(signal_data.get('reasons'), list) else str(signal_data.get('reasons', 'scanner watchlist')),
                    )
                )
            return False

        # ЕСЛИ СИГНАЛ ПРОШЕЛ (Score >= 2.5) — ДОСТАЕМ ОСТАЛЬНЫЕ ДАННЫЕ
        df = signal_data.get('df')
        entry = signal_data.get('entry_price')
        side = signal_data.get('side', 'Buy')
        sl = signal_data.get('sl')
        tp = signal_data.get('tp')

        # Защита от пустых данных: если ключей нет, просто игнорим сигнал.
        # df опционален для быстрых WS-сигналов, где график недоступен.
        if not sl or not tp or entry is None:
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
            "status": "signal_only" if self.signals_only else "pending_order", "pnl_pct": 0.0, "timeframe": signal_data.get('timeframe', '1m'),
            "reasons": reasons_str # <--- ПЕРЕДАЕМ ИХ В БАЗУ ДАННЫХ
        }

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

        if self.signals_only or not self.exchange.session:
            mode = "SIGNALS_ONLY=true" if self.signals_only else "нет биржевой сессии"
            self.logger.info(f"📡 {symbol}: signal-only режим ({mode}) — биржевой ордер не отправлялся.")
            asyncio.create_task(
                self.dashboard.post_log(
                    f"{symbol}: signal-only mode, exchange order was not sent",
                    source="executor",
                    severity="info",
                )
            )
            asyncio.create_task(self._send_execution_report(trade, score, df))
            return True

        order_result = self._place_limit_order(
            symbol=symbol, side=side, qty=size, entry_price=entry, sl_price=sl, tp_price=tp,
            timeframe=signal_data.get('timeframe', '1m'),
        )
        if not order_result:
            self.logger.error(f"❌ Ордер {symbol} не подтвержден биржей. Сделка не записана как активная.")
            return False

        trade.update(order_result)
        trade_id = self.db.add_trade(trade)
        trade["id"] = trade_id
        self.active_trades.append(trade)
        self.order_link_ids.add(trade.get("order_link_id", ""))
        asyncio.create_task(self.dashboard.post_trade(trade))
        asyncio.create_task(self._send_execution_report(trade, score, df))
        return True

    def _sync_exchange_state(self) -> None:
        """Reconcile local active trades with real Bybit positions/orders before trading."""
        if not self.exchange.session:
            return
        try:
            positions = self.exchange.get_positions()
            open_orders = self.exchange.get_open_orders()
            exchange_symbols = {
                item.get("symbol") for item in open_orders if item.get("symbol")
            }
            exchange_symbols.update(
                item.get("symbol") for item in positions if item.get("symbol") and _safe_float(item.get("size")) > 0
            )
            self.order_link_ids.update(
                item.get("orderLinkId") for item in open_orders if item.get("orderLinkId")
            )
            reconciled = []
            for trade in self.active_trades:
                symbol = trade.get("symbol")
                if symbol in exchange_symbols:
                    reconciled.append(trade)
                    continue
                self.logger.warning(f"⚠️ {symbol}: локальная активная сделка не найдена на Bybit; помечаем как reconcile_required.")
                if trade.get("id") is not None:
                    self.db.update_trade_status(trade["id"], "reconcile_required", trade.get("pnl_pct", 0.0) or 0.0)
            self.active_trades = reconciled
            self.logger.info(
                f"🔄 Bybit sync завершен: positions={len(positions)}, open_orders={len(open_orders)}, local_active={len(self.active_trades)}"
            )
        except Exception as exc:
            self.logger.error(f"❌ Не удалось синхронизировать Bybit positions/orders: {exc}")

    def _has_local_active_trade(self, symbol: str) -> bool:
        return any(
            t.get('symbol') == symbol and t.get('status') in {'open', 'pending_order'}
            for t in self.active_trades
        )

    def _has_exchange_duplicate(self, symbol: str) -> bool:
        if self.signals_only or not self.exchange.session:
            return False
        if self.exchange.has_open_position(symbol):
            return True
        return bool(self.exchange.get_open_orders(symbol=symbol))

    def _instrument_rules(self, symbol: str) -> dict | None:
        if symbol not in self.instrument_rules:
            rules = self.exchange.get_instrument_rules(symbol)
            if not rules:
                return None
            self.instrument_rules[symbol] = rules
        return self.instrument_rules[symbol]

    def _floor_to_step(self, value: float, step: str) -> float:
        decimal_step = Decimal(str(step or "0"))
        if decimal_step <= 0:
            return float(value)
        decimal_value = Decimal(str(value))
        return float((decimal_value / decimal_step).to_integral_value(rounding=ROUND_DOWN) * decimal_step)

    def _format_price(self, price: float, rules: dict) -> float:
        return self._floor_to_step(price, rules.get("tick_size", "0"))

    def _format_qty(self, qty: float, entry_price: float, rules: dict | None = None) -> float:
        if qty <= 0:
            return 0.0
        if rules:
            return self._floor_to_step(qty, rules.get("qty_step", "0"))
        if entry_price < 10:
            return max(int(qty), 1)
        if entry_price < 1000:
            return round(qty, 2)
        return round(qty, 3)

    def _make_order_link_id(self, symbol: str, timeframe: str) -> str:
        return f"cv3-{symbol}-{timeframe}-{uuid.uuid4().hex[:18]}"[:36]

    def _place_limit_order(self, symbol, side, qty, entry_price, sl_price, tp_price, timeframe="1m"):
        try:
            if not trading_enabled():
                self.logger.warning(f"🛡️ trading_enabled=false. Пропускаем _place_limit_order для {symbol}")
                return None
            rules = self._instrument_rules(symbol)
            if not rules:
                self.logger.error(f"❌ {symbol}: нет instrument rules, ордер запрещен.")
                return None

            formatted_qty = self._format_qty(qty, entry_price, rules)
            min_qty = float(rules.get("min_qty") or 0.0)
            if formatted_qty < min_qty or formatted_qty <= 0:
                self.logger.error(f"❌ {symbol}: qty {formatted_qty} меньше min_qty {min_qty}")
                return None

            # Для Short цена лимитки должна быть чуть ниже текущей, для Long - чуть выше
            limit_offset = 0.999 if side == "Sell" else 1.001
            limit_price = self._format_price(entry_price * limit_offset, rules)
            stop_loss = self._format_price(sl_price, rules)
            take_profit = self._format_price(tp_price, rules)

            min_notional = float(rules.get("min_notional") or 0.0)
            notional = formatted_qty * limit_price
            if min_notional and notional < min_notional:
                self.logger.error(f"❌ {symbol}: notional {notional:.4f} меньше min_notional {min_notional}")
                return None

            order_link_id = self._make_order_link_id(symbol, timeframe)
            if order_link_id in self.order_link_ids:
                self.logger.error(f"❌ Дубликат orderLinkId {order_link_id}")
                return None

            order_response = self.exchange.session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=str(formatted_qty),
                price=str(limit_price),
                stopLoss=str(stop_loss),
                takeProfit=str(take_profit),
                positionIdx=0,
                timeInForce="GTC",
                orderLinkId=order_link_id,
            )
            if order_response.get("retCode") != 0:
                self.logger.error(f"❌ Bybit отклонил ордер {symbol}: {order_response}")
                return None

            order_id = order_response.get('result', {}).get('orderId')
            status_info = self.exchange.get_order_status(symbol, order_id=order_id, order_link_id=order_link_id)
            has_position = self.exchange.has_open_position(symbol)
            order_status = (status_info or {}).get("orderStatus", "")
            if not has_position and order_status not in {"New", "PartiallyFilled", "Filled", "Created", "Untriggered"}:
                self.logger.error(f"❌ {symbol}: ордер без подтвержденного статуса: {status_info}")
                return None

            local_status = "open" if has_position or order_status in {"Filled", "PartiallyFilled"} else "pending_order"
            self.logger.info(
                f"📝 Ордер {symbol} ({side}) подтвержден Bybit: ID={order_id}, link={order_link_id}, status={order_status or 'position'}"
            )
            return {
                "order_id": order_id or "",
                "order_link_id": order_link_id,
                "size": formatted_qty,
                "remaining_size": formatted_qty,
                "entry": limit_price,
                "sl": stop_loss,
                "tp": take_profit,
                "status": local_status,
            }

        except Exception as e:
            self.logger.error(f"❌ Ошибка ордера {symbol}: {e}")
            return None

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
                close_qty_fmt = self._format_qty(close_qty, trade['entry'])
                if close_qty_fmt <= 0:
                    self.logger.warning(f"⚠️ {symbol}: TP1 quantity became zero after formatting; skip partial close.")
                    continue

                try:
                    if trading_enabled() and self.exchange.session:
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
        asyncio.create_task(self.dashboard.post_trade(trade))

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
            
            if df is None:
                await self.notifier.send_message(msg)
                return

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

def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
