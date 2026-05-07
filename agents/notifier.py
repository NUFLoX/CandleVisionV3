# -*- coding: utf-8 -*-
import aiohttp
import logging

class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        self.logger = logging.getLogger("CandleVision.Notifier")

    async def send_message(self, text: str):
        """Асинхронная отправка сообщения в Telegram"""
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, json=payload) as response:
                    if response.status != 200:
                        self.logger.error(f"❌ Ошибка отправки в TG: {await response.text()}")
        except Exception as e:
            self.logger.error(f"💥 Сбой Telegram Notifier: {e}")

    # === НОВЫЙ МЕТОД ДЛЯ ОТПРАВКИ ФОТО ===
    async def send_photo(self, photo_path: str, caption: str):
        try:
            data = aiohttp.FormData()
            data.add_field('chat_id', self.chat_id)
            data.add_field('caption', caption)
            data.add_field('parse_mode', 'HTML')
            data.add_field('photo', open(photo_path, 'rb'))

            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url_photo, data=data) as response:
                    if response.status != 200:
                        self.logger.error(f"❌ Ошибка отправки фото: {await response.text()}")
        except Exception as e:
            self.logger.error(f"💥 Сбой отправки фото: {e}")