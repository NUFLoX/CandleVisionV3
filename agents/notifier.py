# -*- coding: utf-8 -*-
import aiohttp
import logging
from pathlib import Path


class TelegramNotifier:
    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.bot_token = (bot_token or "").strip()
        self.chat_id = (chat_id or "").strip()
        self.enabled = bool(self.bot_token and self.chat_id)
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        self.api_url_photo = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
        self.logger = logging.getLogger("CandleVision.Notifier")

    async def send_message(self, text: str):
        """Асинхронная отправка сообщения в Telegram."""
        if not self.enabled:
            self.logger.debug("Telegram notifier disabled: token/chat_id are empty.")
            return

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.api_url, json=payload) as response:
                    if response.status != 200:
                        self.logger.error(f"❌ Ошибка отправки в TG: {await response.text()}")
        except Exception as e:
            self.logger.error(f"💥 Сбой Telegram Notifier: {e}")

    async def send_photo(self, photo_path: str, caption: str):
        """Отправляет картинку с подписью; если фото недоступно — отправляет текст."""
        if not self.enabled:
            self.logger.debug("Telegram notifier disabled: token/chat_id are empty.")
            return

        path = Path(photo_path)
        if not path.exists():
            await self.send_message(caption)
            return

        try:
            data = aiohttp.FormData()
            data.add_field("chat_id", self.chat_id)
            data.add_field("caption", caption)
            data.add_field("parse_mode", "HTML")
            with path.open("rb") as photo:
                data.add_field("photo", photo, filename=path.name, content_type="image/png")
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.api_url_photo, data=data) as response:
                        if response.status != 200:
                            self.logger.error(f"❌ Ошибка отправки фото: {await response.text()}")
                            await self.send_message(caption)
        except Exception as e:
            self.logger.error(f"💥 Сбой отправки фото: {e}")
            await self.send_message(caption)
