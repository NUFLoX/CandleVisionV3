# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv

# Загружаем ключи из скрытого файла .env
load_dotenv()

# --- ТВОИ КЛЮЧИ (безопасный импорт) ---
# Убедись, что в .env файле переменные называются именно так: 
# TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ==========================================
# ⚙️ ГЛОБАЛЬНЫЕ НАСТРОЙКИ CANDLEVISION
# ==========================================

# --- Настройки API и сети ---
API_RETRY_COUNT = 3
API_TIMEOUT = 10
SCAN_DELAY = 0.2

# --- Тайминги Оркестратора ---
SCAN_INTERVAL = 60
WATCHLIST_INTERVAL = 180
DANGER_PAUSE = 180

# --- Настройки Торговли ---
RISK_PERCENT = 1.0
INITIAL_BALANCE = 1000
SCORE_TO_WATCH = 1.5
SCORE_TO_TRADE = 2.5