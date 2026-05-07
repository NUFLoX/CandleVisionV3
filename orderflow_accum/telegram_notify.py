from __future__ import annotations

import logging
from pathlib import Path

import aiohttp

logger = logging.getLogger("Accum.Telegram")


class TelegramNotifier:
    def __init__(self, token: str | None, chat_id: str | None):
        self.token = token
        self.chat_id = chat_id

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def send_message(self, text: str) -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as response:
                    if response.status >= 400:
                        body = await response.text()
                        logger.warning("Telegram send failed with status %s: %s", response.status, body[:300])
        except Exception as exc:
            logger.warning("Telegram send failed: %r", exc)

    async def send_photo(self, photo_path: str, caption: str) -> None:
        if not self.enabled:
            return
        path = Path(photo_path)
        if not path.exists():
            await self.send_message(caption)
            return
        url = f"https://api.telegram.org/bot{self.token}/sendPhoto"
        try:
            timeout = aiohttp.ClientTimeout(total=45)
            form = aiohttp.FormData()
            form.add_field("chat_id", self.chat_id)
            form.add_field("caption", caption)
            form.add_field("parse_mode", "HTML")
            with path.open("rb") as handle:
                form.add_field("photo", handle, filename=path.name, content_type="image/png")
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, data=form) as response:
                        if response.status >= 400:
                            body = await response.text()
                            logger.warning("Telegram sendPhoto failed with status %s: %s", response.status, body[:300])
                            await self.send_message(caption)
        except Exception as exc:
            logger.warning("Telegram sendPhoto failed: %r", exc)
            await self.send_message(caption)

    async def send_signal(
        self,
        symbol: str,
        side: str,
        entry: float,
        stop_loss: float,
        take_profit_1: float,
        take_profit_2: float,
        reasons: list[str] | tuple[str, ...] | None = None,
        photo_path: str | None = None,
        title: str | None = None,
    ) -> None:
        reasons_text = ", ".join(reasons or []) or "n/a"
        header = title or "Accumulation signal"
        text = (
            f"📡 <b>{header}</b>\n"
            f"<b>{symbol}</b> | <b>{side}</b>\n"
            f"Entry: <code>{entry:.8f}</code>\n"
            f"SL: <code>{stop_loss:.8f}</code>\n"
            f"TP1: <code>{take_profit_1:.8f}</code>\n"
            f"TP2: <code>{take_profit_2:.8f}</code>\n"
            f"Reasons: {reasons_text}"
        )
        if photo_path:
            await self.send_photo(photo_path, text)
        else:
            await self.send_message(text)
