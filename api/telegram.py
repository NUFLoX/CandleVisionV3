# -*- coding: utf-8 -*-
import aiohttp
import logging
from config.settings import TOKEN, CHAT_ID

class TelegramReporter:
    def __init__(self):
        self.logger = logging.getLogger("CandleVision.Telegram")
        self.base_url = f"https://api.telegram.org/bot{TOKEN}"

    async def send_message_async(self, text: str):
        """Асинхронная отправка сообщения в Telegram."""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML"
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        resp_text = await response.text()
                        self.logger.error(f"❌ Ошибка Telegram: {resp_text}")
                    else:
                        self.logger.info("📡 Уведомление отправлено в Telegram.")
        except Exception as e:
            self.logger.error(f"❌ Сбой сети при отправке в Telegram: {e}")

    async def send_photo_async(self, photo_path: str, caption: str):
        """Отправляет картинку с подписью в Telegram."""
        # parse_mode передаём как GET-параметр, НЕ в FormData!
        url = f"{self.base_url}/sendPhoto?chat_id={CHAT_ID}&parse_mode=HTML"
        try:
            async with aiohttp.ClientSession() as session:
                with open(photo_path, 'rb') as photo:
                    data = aiohttp.FormData()
                    data.add_field('photo', photo)
                    data.add_field('caption', caption)
                    # parse_mode уже в URL, не добавляем в FormData!

                    async with session.post(url, data=data) as response:
                        if response.status != 200:
                            resp_text = await response.text()
                            self.logger.error(f"❌ Ошибка отправки фото: {resp_text}")
                        else:
                            self.logger.info("📸 График успешно отправлен в ТГ.")
        except Exception as e:
            self.logger.error(f"❌ Сбой сети (Telegram Фото): {e}")

    def format_trade_report(self, trade, score):
        """Формирует красивый текст отчета для Telegram."""
        direction = trade.get("direction", "LONG")

        if direction == "LONG":
            dir_text = "🟢 <b>LONG</b> (✅ Подходит для Спота и Фьючерсов)"
            market_note = "<i>*Спотовые трейдеры могут просто купить эту монету по цене входа.</i>"
        else:
            dir_text = "🔴 <b>SHORT</b> (⚠️ Только Фьючерсы)"
            market_note = "<i>*Спот-трейдеры пропускают этот сигнал (на споте нельзя шортить).</i>"

        report = (
            f"⚡️ <b>СИГНАЛ ДОЗРЕЛ | Score: {score}</b>\n\n"
            f"🪙 <b>Монета:</b> #{trade['symbol']}\n"
            f"🎯 <b>Направление:</b> {dir_text}\n"
            f"⏱ <b>Таймфрейм:</b> {trade['timeframe']}\n"
            f"───────────────\n"
            f"💵 <b>Вход (Entry):</b> {trade['entry']}\n"
            f"🛑 <b>Stop Loss:</b> {trade['sl']}\n"
            f"✅ <b>Take Profit:</b> {trade['tp']}\n"
            f"───────────────\n"
            f"{market_note}"
        )
        return report