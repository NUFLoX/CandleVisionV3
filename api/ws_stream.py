# -*- coding: utf-8 -*-
import json
import logging
import asyncio
import websockets
import time

class OrderBookStream:
    def __init__(self, testnet=True, executor=None):
        self.logger = logging.getLogger("CandleVision.WS")
        self.ws_url = "wss://stream-testnet.bybit.com/v5/public/linear" if testnet else "wss://stream.bybit.com/v5/public/linear"
        self.ws = None
        self.symbols = []
        
        # Ссылка на Экзекутор для отправки снайперских сигналов
        self.executor = executor 
        
        # Хранилище стаканов
        self.orderbooks = {} 
        
        # СНАЙПЕРСКИЕ ЦЕЛИ: {"XRPUSDT": 0.6500}
        self.sniper_targets = {} 

    async def connect(self, symbols: list):
        self.symbols = symbols
        retry_delay = 5
        max_delay = 120

        while True:
            try:
                self.logger.info("🔌 Подключение к Bybit WebSockets (Orderbook)...")
                async with websockets.connect(
                    self.ws_url, 
                    ping_interval=20, 
                    ping_timeout=10,
                    close_timeout=5
                ) as self.ws:
                    self.logger.info("🟢 WebSocket соединение установлено!")
                    
                    # Разбиваем подписку на батчи по 10 символов (лимит Bybit)
                    batch_size = 10
                    for i in range(0, len(symbols), batch_size):
                        batch = symbols[i:i + batch_size]
                        req = {
                            "op": "subscribe",
                            "args": [f"orderbook.50.{s}" for s in batch]
                        }
                        await self.ws.send(json.dumps(req))
                        await asyncio.sleep(0.2) # Небольшая пауза между батчами

                    # Запускаем пинг и слушателя
                    ping_task = asyncio.create_task(self._keepalive())
                    try:
                        await self._listen()
                    except websockets.ConnectionClosed as e:
                        self.logger.error(f"🔴 Обрыв WS соединения (Orderbook): {e}. Переподключение через {retry_delay}с...")
                    finally:
                        ping_task.cancel()
                        
            except Exception as e:
                self.logger.error(f"🔴 Ошибка подключения WS: {e}. Переподключение через {retry_delay}с...")
            
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)

    async def _keepalive(self):
        """Фоновый пинг для удержания соединения"""
        while True:
            await asyncio.sleep(20)
            if self.ws and not self.ws.closed:
                try:
                    await self.ws.send(json.dumps({"op": "ping"}))
                except:
                    pass

    async def _listen(self):
        """Слушаем и обновляем стаканы в памяти + Ждем пробоя в ленте сделок"""
        async for message in self.ws:
            data = json.loads(message)
            topic = data.get('topic', '')
            
            # --- 1. ОБРАБОТКА СТАКАНА (Обычный режим для Сканера) ---
            if topic.startswith('orderbook'):
                symbol = data['data']['s']
                type_ = data['type'] 
                
                if type_ == 'snapshot':
                    self.orderbooks[symbol] = {
                        'bids': {float(p): float(v) for p, v in data['data']['b']},
                        'asks': {float(p): float(v) for p, v in data['data']['a']},
                        'update_time': time.time()
                    }
                
                elif type_ == 'delta' and symbol in self.orderbooks:
                    for p, v in data['data']['b']:
                        price, vol = float(p), float(v)
                        if vol == 0:
                            self.orderbooks[symbol]['bids'].pop(price, None)
                        else:
                            self.orderbooks[symbol]['bids'][price] = vol
                            
                    for p, v in data['data']['a']:
                        price, vol = float(p), float(v)
                        if vol == 0:
                            self.orderbooks[symbol]['asks'].pop(price, None)
                        else:
                            self.orderbooks[symbol]['asks'][price] = vol
                            
                    self.orderbooks[symbol]['update_time'] = time.time()

            # --- 2. ОБРАБОТКА ЛЕНТЫ СДЕЛОК (Снайперский режим) ---
            elif topic.startswith('publicTrade'):
                symbol = data.get('data', [{}])[0].get('s')
                
                # Проверяем, есть ли эта монета в нашем прицеле
                if symbol in self.sniper_targets:
                    res_level = self.sniper_targets[symbol]
                    
                    # В ленте может прилететь сразу пачка сделок, проверяем каждую
                    for trade in data['data']:
                        price = float(trade['p'])
                        vol = float(trade['v'])
                        side = trade['S'] # 'Buy' или 'Sell'
                        volume_usd = price * vol
                        
                        # ТРИГГЕР ПРОБОЯ: Цена выше уровня И это рыночная Покупка И объем > $10,000
                        if price >= res_level and side == 'Buy' and volume_usd > 10_000:
                            self.logger.critical(f"💥 ПРОБОЙ НАКОПЛЕНИЯ {symbol}! Уровень {res_level} снесен китом (Объем: ${volume_usd:,.0f})!")
                            
                            if self.executor:
                                signal_data = {
                                    "symbol": symbol,
                                    "side": "Buy",
                                    "entry": price,
                                    "reasons": ["SmartMoney Breakout", "WS Sniper"]
                                }
                                # Моментальный выстрел!
                                asyncio.create_task(self.executor.process_signal_async(signal_data))
                            else:
                                self.logger.error("❌ Экзекутор не подключен к WS! Выстрел вхолостую.")
                            
                            # Убираем цель из памяти, чтобы не спамить ордерами
                            del self.sniper_targets[symbol]
                            break # Выходим из цикла, выстрел уже сделан

    def get_orderbook(self, symbol):
        """Возвращает актуальный снимок стакана для Сканера"""
        ob = self.orderbooks.get(symbol)
        if not ob:
            return None
            
        # Защита от устаревших данных (если стакан не обновлялся дольше 10 сек)
        if time.time() - ob['update_time'] > 10:
            return None

        # Сортируем словари: bids по убыванию (лучшая цена сверху), asks по возрастанию
        sorted_bids = sorted(ob['bids'].items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(ob['asks'].items(), key=lambda x: x[0])
        
        return {"bids": sorted_bids, "asks": sorted_asks}
    
    def get_imbalance(self, symbol, depth=20):
        """Возвращает дисбаланс (OFI) стакана для совместимости со стратегиями."""
        ob = self.get_orderbook(symbol)
        if not ob or not ob.get('bids') or not ob.get('asks'):
            return 0.0

        bids = ob['bids'][:depth]
        asks = ob['asks'][:depth]

        bid_vol = sum(v for p, v in bids)
        ask_vol = sum(v for p, v in asks)
        total_vol = bid_vol + ask_vol

        if total_vol == 0:
            return 0.0

        # Возвращаем OFI от -1.0 до 1.0
        return (bid_vol - ask_vol) / total_vol
    
    async def subscribe(self, symbols):
        """Динамическая подписка на новые монеты (вызывается Экзекутором или Дожимом)"""
        if not self.ws or self.ws.closed:
            return
            
        # Если передали одну строку (например, "BTCUSDT"), превращаем в список
        if isinstance(symbols, str):
            symbols = [symbols]
            
        # Фильтруем монеты: подписываемся только на те, которых еще нет в нашем пуле
        new_symbols = [s for s in symbols if s not in self.symbols]
        
        if new_symbols:
            self.symbols.extend(new_symbols)
            # Разбиваем на батчи по 10, как требует Bybit
            for i in range(0, len(new_symbols), 10):
                batch = new_symbols[i:i + 10]
                req = {
                    "op": "subscribe",
                    "args": [f"orderbook.50.{s}" for s in batch]
                }
                await self.ws.send(json.dumps(req))
                await asyncio.sleep(0.1)
            self.logger.info(f"📡 Дополнительно подписались на стаканы: {new_symbols}")

    def add_sniper_target(self, symbol, resistance_level):
        """Принимает уровень пробоя от макро-сканера и подписывается на ленту"""
        self.sniper_targets[symbol] = float(resistance_level)
        self.logger.warning(f"🎯 СНАЙПЕР ВЗВЕДЕН: {symbol} | Жду пробоя уровня {resistance_level}")
        
        # Чтобы поймать удар рыночным ордером, подписываемся на publicTrade
        if self.ws and not self.ws.closed:
            req = {
                "op": "subscribe",
                "args": [f"publicTrade.{symbol}"]
            }
            # Отправляем запрос асинхронно, не блокируя основной поток
            asyncio.create_task(self.ws.send(json.dumps(req)))
            self.logger.info(f"🔌 Снайпер подключился к ленте сделок: {symbol}")