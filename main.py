# -*- coding: utf-8 -*-
import asyncio
import logging

# 1. Настройка логирования (пишем и в консоль, и в файл)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Импорты модулей ядра
from core.orchestrator import Orchestrator
from core.executor import Executor
from core.sentinel import Sentinel
from core.database import Database
from scout.scanner import Scout
from api.market import fetch_all_usdt_pairs_async
from api.ws_stream import OrderBookStream

# Импорт стратегий
from core.triup import TriUpManager
from agents.macro import SmartMoneyTracker
from api.bybit_client import BybitClient # Убедись, что тут твой класс работы с API
from agents.brain import AutoBrain
from agents.sonar import VolumeSonar
from scout.strategies.classic import strategy_classic
from scout.strategies.pump import strategy_pump
from scout.strategies.squeeze import strategy_squeeze
from agents.sentinel_sentiment import SentimentAgent
from agents.sentinel_telegram import TelegramScout
from agents.sentinel_tape import TapeReader
from config.settings import BYBIT_TESTNET

async def main():
    print("🚀 Запуск HFT-ядра CandleVision (Full Stack: DB + WS + Async)...")
    
    # 2. Инициализация неубиваемых сервисов (Они живут вне цикла перезапуска)
    signal_queue = asyncio.Queue()
    db = Database()
    
    strategies = [strategy_classic, strategy_pump, strategy_squeeze]
    
    executor = Executor(db=db, initial_balance=1000)
    executor.queue = signal_queue 
    sentinel = Sentinel()
    
    # 3. Динамическая загрузка пар
    print("📡 Получаем актуальный список всех USDT пар с Bybit...")
    all_symbols = await fetch_all_usdt_pairs_async()
    
    if not all_symbols:
        print("❌ Критическая ошибка: не удалось загрузить список пар.")
        return
        
    print(f"✅ Готово к сканированию {len(all_symbols)} пар.")

    # ========================================================
    # 🛡️ ГЛАВНЫЙ ЦИКЛ ВЫЖИВАНИЯ (ЗАЩИТА ОТ ОБРЫВОВ СЕТИ)
    # ========================================================
    try:
        while True:
            # Инициализируем WS и Сканер внутри цикла! 
            # Если сеть упала, при рестарте мы получим абсолютно чистые и новые сокеты
            ws_stream = OrderBookStream(executor=executor) # <--- ПЕРЕДАЛИ ЭКЗЕКУТОР ДЛЯ СНАЙПЕРА

            # --- ЗАДЕЛ НА БУДУЩЕЕ (CryptoPanic API) ---
            # sentiment_agent = SentimentAgent(api_key="твой_токен_сюда")
            # scout = Scout(queue=signal_queue, strategies=strategies, ws_stream=ws_stream, sentiment_agent=sentiment_agent)

            # --- TELEGRAM ALPHA ---
            #API_ID = 31253660  # Вставь свои цифры (без кавычек)
            #API_HASH = "01f8061d064a53d40526587222bef6c4"
            #tg_agent = TelegramScout(api_id=API_ID, api_hash=API_HASH)
            
            #print("\n⏳ Ожидание авторизации Telegram (введи номер и код, если просит)...")
            #await tg_agent.client.start() 
            #print("✅ Telegram готов!\n")

            # --- TAPE READER (Отслеживание китов) ---
            # Порог 50,000$ (можно менять)
            tape_agent = TapeReader(volume_threshold=50000)
            
            # Передаем tg_agent в Скаута
            scout = Scout(queue=signal_queue, strategies=strategies, ws_stream=ws_stream)
            scout.load_symbols(all_symbols)
            
            orchestrator = Orchestrator(scout, executor, sentinel, ws_stream)

            # --- ПОДКЛЮЧАЕМ СОНАР ---
            sonar = VolumeSonar(notifier=executor.notifier)
        
            # Запускаем Оркестратор и Сонар параллельно
            orchestrator_task = asyncio.create_task(orchestrator.start())
            sonar_task = asyncio.create_task(sonar.run_loop(scout, tape_agent))

            # Создаем мозг
            brain = AutoBrain(db_path="candlevision.db", scan_interval_hours=72)
            brain_task = asyncio.create_task(brain.run_loop(scout))

            # --- ЗАПУСК МАКРО-ТРЕКЕРА ---
            api_client = BybitClient(testnet=BYBIT_TESTNET) # Создаем клиент для скачивания истории свечей
            macro_tracker = SmartMoneyTracker(api_client=api_client, scan_interval_hours=2)
            macro_task = asyncio.create_task(macro_tracker.run_loop(all_symbols, ws_stream, executor.notifier))

            # --- ЗАПУСК triUP (ЗАЩИТА И БЕЗУБЫТОК) ---
            triup_manager = TriUpManager(api_client=api_client, db=db, telegram_bot=None)
            triup_task = asyncio.create_task(triup_manager.run_loop())

            try:
                # Запускаем все фоновые процессы разом
                await asyncio.gather(
                    orchestrator_task, 
                    sonar_task, 
                    brain_task,
                    macro_task,
                    triup_task  # <--- Добавили вот эту строку
                )
                break
                
            except Exception as e:
                logging.error(f"💥 Обрыв сети или сбой сокетов: {e}")
                logging.info("🔄 Авто-перезапуск соединения через 10 секунд...")
                
                # Принудительно убиваем зависший сокет перед рестартом
                if hasattr(ws_stream, 'ws') and ws_stream.ws:
                    try:
                        await ws_stream.ws.close()
                    except:
                        pass
                        
                await asyncio.sleep(10) # Пауза перед новой попыткой подключения
                
    except asyncio.CancelledError:
        logging.info("🛑 Получен системный сигнал остановки.")
        
    finally:
        # Блок мягкой остановки (Сработает ТОЛЬКО при ручном выключении Ctrl+C)
        logging.info("🧹 Запуск процедуры мягкой остановки (Graceful Shutdown)...")
        
        # КРИТИЧНО: Безопасно закрываем базу данных
        if hasattr(db, 'conn') and db.conn:
            db.conn.close()
            logging.info("💾 Соединение с SQLite успешно закрыто.")
            
        logging.info("👋 Все процессы корректно завершены. До свидания!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Перехват Ctrl+C
        print("\n🛑 Бот остановлен вручную (Ctrl+C). Сохраняем состояние...")