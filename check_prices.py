# -*- coding: utf-8 -*-
import re
import requests

def check_market_results(filepath="trading.log"):
    # Топ-5 монет из твоего списка
    target_coins = ["SONICUSDT", "BSBUSDT", "0GUSDT", "PENGUUSDT", "1000LUNCUSDT"]
    last_signals = {}

    print("🔍 Читаем логи и ищем последние точки входа...")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if "🚀" in line and "ВХОД:" in line:
                    for coin in target_coins:
                        if coin in line:
                            # Пытаемся вытащить цену входа. Формат: Entry: 0.5464
                            match_entry = re.search(r"Entry:\s+([\d\.]+)", line)
                            # Пытаемся понять направление (если есть)
                            side = "SELL" if "SELL" in line else "BUY" 
                            
                            if match_entry:
                                last_signals[coin] = {
                                    "entry": float(match_entry.group(1)),
                                    "side": side
                                }
    except FileNotFoundError:
        print("❌ Файл trading.log не найден!")
        return

    print("🌐 Запрашиваем актуальные цены с Bybit...\n")
    print("="*60)
    print(f"{'Монета':<15} | {'Вход':<10} | {'Сейчас':<10} | {'Тип':<5} | {'Результат'}")
    print("="*60)

    for coin, data in last_signals.items():
        try:
            url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={coin}"
            res = requests.get(url).json()
            if res.get("retCode") == 0 and res["result"]["list"]:
                current_price = float(res["result"]["list"][0]["lastPrice"])
                entry = data["entry"]
                side = data["side"]
                
                # Считаем разницу в процентах
                diff_pct = ((current_price - entry) / entry) * 100
                
                # Определяем, плюс это или минус
                if side == "BUY":
                    is_profit = diff_pct > 0
                    pnl_str = f"+{diff_pct:.2f}%" if is_profit else f"{diff_pct:.2f}%"
                else: # SELL
                    is_profit = diff_pct < 0
                    pnl_str = f"+{abs(diff_pct):.2f}%" if is_profit else f"-{diff_pct:.2f}%"

                icon = "✅ ПРОФИТ" if is_profit else "❌ УБЫТОК"
                
                print(f"{coin:<15} | {entry:<10.5f} | {current_price:<10.5f} | {side:<5} | {icon} ({pnl_str})")
            else:
                print(f"{coin:<15} | Ошибка получения цены с Bybit")
        except Exception as e:
            print(f"{coin:<15} | Ошибка: {e}")
            
    print("="*60)

if __name__ == "__main__":
    check_market_results()