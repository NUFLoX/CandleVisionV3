# CandleVision HFT Engine 🚀

Асинхронный торговый бот для Bybit V5 с использованием Python `asyncio`.

## Особенности
* **Multi-stage Scanning:** Поиск сигналов по 500+ парам одновременно.
* **Watchlist Refiner:** Логика "дожима" перспективных монет.
* **OrderBook Analytics:** Анализ дисбаланса стаканов через WebSockets.
* **State Persistence:** Сохранение состояния и сделок в SQLite.
* **Auto-Recovery:** Автоматическое переподключение при обрыве связи.

## OrderFlow V1

Новый отдельный модуль для сравнения со старой логикой.

### Что делает
- realtime-анализ стакана `orderbook.50`
- realtime-анализ ленты `publicTrade`
- поиск:
  - absorption near support
  - absorption near resistance
  - breakout with aggressive flow
  - macro accumulation scan every 2 hours on `1H/4H/1D`
- отправляет сигналы в консоль, `orderflow_v1.log` и Telegram

### Запуск
Windows:
```bat
run_orderflow_v1.bat
```

Linux/macOS:
```bash
chmod +x run_orderflow_v1.sh
./run_orderflow_v1.sh
```

### Точка входа
```bash
python orderflow_v1_main.py
```

## CandleVision Dashboard MVP

В репозиторий добавлен стартовый dashboard для панели управления и наблюдения CandleVision.

### Что входит
- **Консоль бота:** live-логи сканера, API, rate-limit, Telegram/X, executor и сделок.
- **Состояние рынка:** `BTC Filter`, `Altcoin Mode`, `Liquidity`, `Market Regime`.
- **Окно сигналов:** карточки сигналов с фильтрами `Strong / Medium / Weak`, `Watchlist / Confirmed / Aggressive`, `Binance / Bybit`, `1h / 4h / 1d`.
- **Market Pressure Strips:** полосы BTC cap, BTC dominance, USDT dominance и TOTAL3.
- **Поиск монеты:** метрики inflow, CEX netflow, whale activity, accumulation score, orderbook imbalance, RSI, ATR%, EMA, support/resistance и Bot Verdict.
- **Bot Health:** scanner, executor, Telegram, X, Bybit/Binance API, database, Redis, rate-limit.

### Архитектура

```text
CandleVision Bot
     ↓
Dashboard Data Hub / future PostgreSQL / future Redis
     ↓
FastAPI Backend + WebSocket
     ↓
React + Tailwind Dashboard
```

MVP использует in-memory `DashboardStore`, чтобы панель можно было запустить сразу. API уже отделён от UI, поэтому хранилище можно заменить на PostgreSQL/Redis без изменения frontend-контракта.

По умолчанию dashboard больше не заполняется демо-сигналами/сделками. При старте он подтягивает реальные публичные данные Bybit (tickers, klines, orderbook) и CoinGecko global для market pressure; сигналы, watchlist и сделки появляются только после ingest от бота. Принудительно обновить live-снимок можно через `POST /api/refresh`.

### Запуск

```bash
pip install -r requirements.txt
uvicorn dashboard.server:app --host 0.0.0.0 --port 8000 --reload
```

После запуска откройте:

```text
http://localhost:8000
```

Swagger/OpenAPI доступен здесь:

```text
http://localhost:8000/docs
```

### Основные API

- `GET /api/status` — здоровье scanner/executor/API/DB/Redis.
- `GET /api/signals` — список сигналов с query-фильтрами `strength`, `signal_type`, `exchange`, `timeframe`.
- `GET /api/market-state` — главный market filter.
- `GET /api/coin/{symbol}` — аналитика монеты, например `API3`, `SOL`, `BTC`.
- `GET /api/logs` — live-логи.
- `GET /api/watchlist` — почти-сигналы 24–48 часов.
- `GET /api/dominance` — BTC.D / USDT.D / TOTAL3 pressure strips.
- `GET /api/trades` — открытые и закрытые сделки.
- `GET /api/snapshot` — полный снимок dashboard.
- `WS /ws` — live-обновления без перезагрузки.

### Интеграция бота с dashboard

Бот может отправлять события напрямую в ingest endpoints:

```bash
curl -X POST http://localhost:8000/api/ingest/log \
  -H 'Content-Type: application/json' \
  -d '{"message":"Bybit rate-limit protection: OK","source":"gateway","severity":"info"}'
```

```bash
curl -X POST http://localhost:8000/api/ingest/signal \
  -H 'Content-Type: application/json' \
  -d '{"id":"api3-1h-001","symbol":"API3USDT","exchange":"Bybit","timeframe":"1h","score":8.7,"strength":"Strong","signal_type":"Confirmed","entry":0.912,"stop_loss":0.848,"take_profit_1":1.04,"reason":"EMA20 up + VSpike q95 + breakout","status":"ACTIVE"}'
```
