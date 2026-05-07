# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import logging
import time

class SentimentAgent:
    def __init__(self, api_key="твой_ключ_cryptopanic"):
        self.logger = logging.getLogger("CandleVision.Sentiment")
        self.api_key = api_key
        self.base_url = "https://cryptopanic.com/api/v1/posts/"
        # Хранилище: {"SOL": {"score": 1.5, "timestamp": 12345}, "BTC": ...}
        self.sentiment_cache = {} 

    async def start_polling(self, symbols_list):
        """Фоновый процесс: мониторинг новостей и X/Twitter каждые 15 секунд"""
        self.logger.info("🐦 Запуск X-Sentiment модуля (CryptoPanic API)...")
        
        # Очищаем тикеры (SOLUSDT -> SOL) для поиска
        base_assets = list(set([sym.replace("USDT", "") for sym in symbols_list]))
        
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    # Запрашиваем только свежие позитивные новости (фильтр rising/bullish можно настроить)
                    params = {
                        "auth_token": self.api_key,
                        "filter": "hot", 
                        "regions": "en",
                        "kind": "news" # Включает media и twitter
                    }
                    
                    async with session.get(self.base_url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            self._parse_news(data.get('results', []), base_assets)
                        else:
                            self.logger.warning(f"Ошибка API Сентимента: {response.status}")
                            
                except Exception as e:
                    self.logger.error(f"❌ Ошибка парсинга новостей: {e}")
                
                await asyncio.sleep(15) # Rate limit у CryptoPanic - 5 запросов в секунду, мы делаем 1 раз в 15 сек

    def _parse_news(self, posts, base_assets):
        """Анализирует посты и начисляет баллы монетам"""
        current_time = time.time()
        
        for post in posts:
            title = post.get('title', '').upper()
            currencies = post.get('currencies', [])
            
            # Если в посте есть привязка к валюте
            if currencies:
                for coin in currencies:
                    code = coin.get('code')
                    if code in base_assets:
                        # Оцениваем хайп. Учитываем встроенные голоса пользователей
                        votes = post.get('votes', {})
                        bullish = votes.get('positive', 0)
                        bearish = votes.get('negative', 0)
                        
                        # Простейшая логика: если позитива больше, даем +1 балл
                        if bullish > bearish + 5:  # Должен быть явный перевес
                            self.sentiment_cache[code] = {
                                'score': 1.0,
                                'timestamp': current_time,
                                'title': post.get('title')
                            }
                            self.logger.info(f"🔥 ХАЙП ДЕТЕКТ: {code} | {post.get('title')}")

    def get_sentiment_bonus(self, symbol):
        """Отдает балл сентимента Сканеру. Если новость старая (> 1 часа), сбрасывает."""
        base_asset = symbol.replace("USDT", "")
        data = self.sentiment_cache.get(base_asset)
        
        if not data:
            return 0.0
            
        # Если новости больше часа (3600 секунд), хайп прошел
        if time.time() - data['timestamp'] > 3600:
            del self.sentiment_cache[base_asset]
            return 0.0
            
        return data['score']