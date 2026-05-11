# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import time
import websockets

from config.settings import BYBIT_WS_PUBLIC_URL

class TapeReader:
    def __init__(self, volume_threshold=50000):
        self.logger = logging.getLogger("CandleVision.Tape")
        self.volume_threshold = volume_threshold  # Порог кита (в долларах)
        self.whale_signals = {}  # Кэш сигналов {symbol: {score, timestamp}}
        self.ws = None

    async def connect(self, symbols):
        url = BYBIT_WS_PUBLIC_URL
        
        try:
            async with websockets.connect(url) as ws:
                self.ws = ws
                self.logger.info("🐋 Подключение к потоку сделок (Tape Reader)...")

                # Подписываемся на сделки пачками по 10 штук
                for i in range(0, len(symbols), 10):
                    batch = symbols[i:i + 10]
                    req = {
                        "op": "subscribe",
                        "args": [f"publicTrade.{s}" for s in batch]
                    }
                    await ws.send(json.dumps(req))
                    await asyncio.sleep(0.1)

                self.logger.info("🟢 Tape Reader успешно подписан на сделки!")

                # Слушаем эфир
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    if "topic" in data and data["topic"].startswith("publicTrade"):
                        for trade in data.get("data", []):
                            symbol = trade["s"]
                            side = trade["S"]  # "Buy" или "Sell"
                            size = float(trade["v"])
                            price = float(trade["p"])
                            volume_usd = size * price

                            # Если сделка больше нашего порога — это КИТ
                            if volume_usd >= self.volume_threshold:
                                emoji = "🟢" if side == "Buy" else "🔴"
                                self.logger.info(f"🚨 КИТ {emoji} [{symbol}] | Удар по рынку: ${volume_usd:,.0f}")
                                
                                # Даем +1.5 балла за покупку китом, и -1.5 за сброс
                                score_mod = 1.5 if side == "Buy" else -1.5
                                self.whale_signals[symbol] = {
                                    'score': score_mod,
                                    'timestamp': time.time()
                                }
        except Exception as e:
            self.logger.error(f"💥 Сбой в потоке сделок (Tape Reader): {e}")

    def get_whale_bonus(self, symbol):
        """Отдает балл сентимента Сканеру. След кита остывает за 1 минуту."""
        data = self.whale_signals.get(symbol)
        
        if not data:
            return 0.0
            
        # Через 60 секунд влияние этой сделки обнуляется
        if time.time() - data['timestamp'] > 60:
            del self.whale_signals[symbol]
            return 0.0
            
        return data['score']