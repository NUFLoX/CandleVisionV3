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
