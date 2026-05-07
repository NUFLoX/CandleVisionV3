# -*- coding: utf-8 -*-
import re
import os

def analyze_logs(filepath="trading.log"):
    total_signals = 0
    successful_orders = 0
    failed_orders = 0
    regulatory_blocks = 0
    
    # Словари для детальной аналитики
    coins = {}
    error_details = []

    if not os.path.exists(filepath):
        print(f"❌ Файл {filepath} не найден.")
        return

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                # 1. Ищем сигналы на вход и фиксируем Score
                if "🚀 ВХОД:" in line:
                    total_signals += 1
                    match = re.search(r"ВХОД:\s+([A-Z0-9]+)\s+\|\s+Score:\s+([\d\.]+)", line)
                    if match:
                        symbol = match.group(1)
                        score = float(match.group(2))
                        coins[symbol] = coins.get(symbol, 0) + 1
                        
                # 2. Считаем блокировки по IP (ошибка 10024)
                elif "ErrCode: 10024" in line or "regulatory restrictions" in line:
                    regulatory_blocks += 1
                    # Пытаемся понять, по какой монете был блок
                    # (обычно ошибка идет сразу после строки с попыткой входа)
                    failed_orders += 1

                # 3. Ищем успешные исполнения
                elif "✅ Боевой ордер" in line:
                    successful_orders += 1
                    
                # 4. Прочие ошибки выставления ордеров
                elif "❌ Ошибка" in line and "10024" not in line:
                    failed_orders += 1

        # ==========================================
        # ВЫВОД РАСШИРЕННОЙ СТАТИСТИКИ
        # ==========================================
        print("\n" + "="*50)
        print("📊 ГЛОБАЛЬНЫЙ АНАЛИЗ ТОРГОВОЙ СЕССИИ 📊")
        print("="*50)
        
        print(f"🎯 Всего качественных сигналов:  {total_signals}")
        print(f"✅ Успешно открыто на бирже:      {successful_orders}")
        print(f"❌ Всего отклонено ордеров:       {failed_orders}")
        
        print("-" * 50)
        print(f"🚫 ИЗ НИХ БЛОК ПО IP (Ош. 10024): {regulatory_blocks}")
        
        # Расчет эффективности
        potential_reach = (total_signals - regulatory_blocks) / total_signals * 100 if total_signals > 0 else 0
        print(f"📈 Доступность рынка (через IP):  {potential_reach:.1f}%")
        
        print("-" * 50)
        if coins:
            print("💎 Топ найденных ракет (по частоте сигналов):")
            for coin, count in sorted(coins.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  • {coin}: {count} раз(а)")
        
        print("="*50)
        
        if regulatory_blocks > 0:
            print("\n💡 СОВЕТ: Твой IP блокирует сделки. Пора внедрять PROXY в код.")
        
    except Exception as e:
        print(f"❌ Ошибка при чтении лога: {e}")

if __name__ == "__main__":
    analyze_logs()