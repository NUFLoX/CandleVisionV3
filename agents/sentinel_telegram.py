# -*- coding: utf-8 -*-
import asyncio
import logging
import time
from telethon import TelegramClient, events

class TelegramScout:
    def __init__(self, api_id, api_hash, session_name='candlevision_tg'):
        self.logger = logging.getLogger("CandleVision.TG")
        self.api_id = api_id
        self.api_hash = api_hash
        self.client = TelegramClient(session_name, self.api_id, self.api_hash)
        
        # Сюда впиши юзернеймы каналов (без @), которые хочешь парсить
        self.target_channels = [
            'binance_announcements', 
            'whale_alert' 
        ] 
        self.tg_cache = {} 

    async def start_listening(self, symbols_list):
        """Запуск клиента и прослушки сообщений"""
        base_assets = list(set([sym.replace("USDT", "") for sym in symbols_list]))
        
        self.logger.info("📱 Запуск Telegram-Скаута...")
        await self.client.start()
        self.logger.info("✅ Telegram юзербот успешно авторизован!")

        @self.client.on(events.NewMessage(chats=self.target_channels))
        async def handler(event):
            text = event.raw_text.upper()
            
            for coin in base_assets:
                if f"${coin}" in text or f" {coin} " in f" {text} ":
                    keywords = ["LISTING", "PARTNERSHIP", "LAUNCH", "MAINNET", "AIRDROP"]
                    if any(kw in text for kw in keywords):
                        self.logger.info(f"🚀 TG ИНСАЙД ПО {coin}! Канал: {event.chat.username if event.chat else 'Private'}")
                        self.tg_cache[coin] = {'score': 2.0, 'timestamp': time.time()}

        await self.client.run_until_disconnected()

    def get_tg_bonus(self, symbol):
        """Отдает балл сентимента Сканеру."""
        base_asset = symbol.replace("USDT", "")
        data = self.tg_cache.get(base_asset)
        
        if not data:
            return 0.0
            
        if time.time() - data['timestamp'] > 300:
            del self.tg_cache[base_asset]
            return 0.0
            
        return data['score']