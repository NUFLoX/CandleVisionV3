# -*- coding: utf-8 -*-
import aiohttp
import pandas as pd
import logging
import asyncio
import ta

from config.settings import API_RETRY_COUNT, API_TIMEOUT, BYBIT_REST_BASE_URL

logger = logging.getLogger("CandleVision.API")

async def fetch_ohlcv_bybit_async(symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
    """Асинхронное скачивание с жесткими таймаутами сокетов."""
    url = f"{BYBIT_REST_BASE_URL}/v5/market/kline"

    bybit_interval = interval
    if bybit_interval.endswith('m'):
        bybit_interval = bybit_interval.replace('m', '')
    elif bybit_interval == '1h':
        bybit_interval = '60'
    elif bybit_interval == '4h':
        bybit_interval = '240'
    elif bybit_interval in ('1d', 'D'):
        bybit_interval = 'D'

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": bybit_interval,
        "limit": limit
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }

    # ЖЕСТКИЙ ТАЙМАУТ: рвем соединение на уровне сокета через 10 секунд
    timeout = aiohttp.ClientTimeout(
        total=API_TIMEOUT,
        sock_connect=10, # Ожидание подключения
        sock_read=10     # Ожидание получения данных от Bybit
    )

    for attempt in range(API_RETRY_COUNT):
        try:
            # Отключаем TCP Keep-Alive и пулы соединений для чистых запросов
            connector = aiohttp.TCPConnector(force_close=True, limit=100)
            async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:
                async with session.get(url, params=params) as response:

                    if response.status == 429:
                        wait_time = 3 * (attempt + 1)
                        logger.warning(f"⚠️ Бан IP (429). Ждем {wait_time} сек... ({symbol})")
                        await asyncio.sleep(wait_time)
                        continue

                    if response.status == 200:
                        data = await response.json()

                        if data.get("retCode") == 0:
                            klines = data["result"]["list"]
                            if not klines:
                                return pd.DataFrame()

                            df = pd.DataFrame(
                                klines,
                                columns=["time", "open", "high", "low", "close", "VOLUME", "turnover"]
                            )
                            df = df.iloc[::-1].reset_index(drop=True)
                            for col in ["open", "high", "low", "close", "VOLUME"]:
                                df[col] = df[col].astype(float)
                            df['time'] = df['time'].astype('int64')

                            # Расчет индикаторов
                            try:
                                df['RSI'] = ta.momentum.rsi(df['close'], window=14)
                                df['MA7'] = ta.trend.sma_indicator(df['close'], window=7)
                                df['MA20'] = ta.trend.sma_indicator(df['close'], window=20)
                                df['MA50'] = ta.trend.sma_indicator(df['close'], window=50)
                                df['EMA50'] = ta.trend.ema_indicator(df['close'], window=50)
                                df['ATR20'] = ta.volatility.average_true_range(
                                    df['high'], df['low'], df['close'], window=20
                                )

                                macd = ta.trend.MACD(df['close'])
                                df['MACDh'] = macd.macd_diff()

                                # Squeeze Momentum
                                bb = ta.volatility.BollingerBands(
                                    close=df['close'], window=20, window_dev=2.0
                                )
                                df['BB_upper'] = bb.bollinger_hband()
                                df['BB_lower'] = bb.bollinger_lband()

                                kc = ta.volatility.KeltnerChannel(
                                    high=df['high'], low=df['low'],
                                    close=df['close'], window=20, window_atr=1.5
                                )
                                df['KC_upper'] = kc.keltner_channel_hband()
                                df['KC_lower'] = kc.keltner_channel_lband()

                                df['SQZ_ON'] = (
                                    (df['BB_upper'] < df['KC_upper']) &
                                    (df['BB_lower'] > df['KC_lower'])
                                )

                                highest_high = df['high'].rolling(window=20).max()
                                lowest_low = df['low'].rolling(window=20).min()
                                avg_price = (highest_high + lowest_low) / 2
                                df['MOMENTUM'] = df['close'] - ((avg_price + df['MA20']) / 2)

                            except Exception as e:
                                logger.error(f"❌ Ошибка расчета индикаторов для {symbol}: {e}")
                                return pd.DataFrame()

                            return df

                        elif data.get("retCode") in [10006, 10018]:
                            wait_time = 2 * (attempt + 1)
                            logger.warning(
                                f"⏳ Уперлись в лимит Bybit. Пауза {wait_time} сек... "
                                f"(Попытка {attempt+1}/{API_RETRY_COUNT})"
                            )
                            await asyncio.sleep(wait_time)
                            continue

                        else:
                            logger.error(f"❌ Ошибка API ({symbol}): {data.get('retMsg')}")
                            return pd.DataFrame()

        except asyncio.TimeoutError:
            logger.error(f"⏱ Таймаут запроса к Bybit ({symbol}). Попытка {attempt+1}")
            await asyncio.sleep(2)  # Фиксированная пауза при таймауте
        except aiohttp.ClientError as e:
            logger.error(f"🔌 Ошибка сети (ClientError) при скачивании {symbol}: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"❌ Системная ошибка при скачивании {symbol}: {e}")
            await asyncio.sleep(2)

    return pd.DataFrame()


async def fetch_all_usdt_pairs_async(min_turnover_usdt=15000000) -> list:
    """Получает список ликвидных пар. Тоже с жестким таймаутом."""
    url = f"{BYBIT_REST_BASE_URL}/v5/market/tickers"
    params = {"category": "linear"}
    
    timeout = aiohttp.ClientTimeout(total=API_TIMEOUT, sock_connect=10, sock_read=10)

    try:
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("retCode") == 0:
                        symbols = []
                        total_pairs = 0
                        for item in data["result"]["list"]:
                            if item['symbol'].endswith('USDT'):
                                total_pairs += 1
                                turnover = float(item.get('turnover24h', 0))
                                if turnover >= min_turnover_usdt:
                                    symbols.append(item['symbol'])
                        
                        logger.info(f"🗑 Отсеяно {total_pairs - len(symbols)} монет с оборотом < ${min_turnover_usdt:,.0f}")
                        return symbols
    except asyncio.TimeoutError:
        logger.error(f"⏱ Таймаут получения списка пар.")
    except Exception as e:
        logger.error(f"❌ Ошибка при получении списка пар: {e}")

    return []