# -*- coding: utf-8 -*-
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from api.bybit_client import BybitClient

print("=" * 60)
print("🧪 ТЕСТ ПОДКЛЮЧЕНИЯ К BYBIT API")
print("=" * 60)

# Создаем клиент для TESTNET
print("\n1️⃣ Инициализируем клиент для TESTNET...")
client = BybitClient(testnet=True)

# Проверка 1: Session создан?
if client.session:
    print("✅ Session создан успешно!")
    
    # Проверка 2: Получить баланс
    print("\n2️⃣ Получаем баланс кошелька...")
    try:
        balance = client.session.get_wallet_balance(accountType="UNIFIED")
        
        if balance.get('retCode') == 0:
            print("✅ Баланс получен успешно!")
            print(f"\n📊 Результат:")
            
            # Парсим результат
            wallet_list = balance.get('result', {}).get('list', [])
            if wallet_list:
                for wallet in wallet_list:
                    coins = wallet.get('coin', [])
                    if coins:
                        print(f"\n   Кошелек {wallet.get('walletType')}:\"")
                        for coin in coins[:5]:  # Первые 5 монет
                            balance_val = coin.get('walletBalance', '0')
                            coin_name = coin.get('coin', 'N/A')
                            print(f"      {coin_name}: {balance_val}")
                        if len(coins) > 5:
                            print(f"      ... и еще {len(coins)-5} монет")
            else:
                print("   Кошельки пусты")
                print(f"   Raw: {balance}")
        else:
            print(f"❌ Ошибка API: {balance.get('retMsg')}")
            print(f"   Code: {balance.get('retCode')}")
            
    except Exception as e:
        print(f"❌ Ошибка получения баланса: {e}")
        print(f"   Type: {type(e).__name__}")
else:
    print("❌ Session не инициализирован")
    print("   Проверь BYBIT_API_KEY и BYBIT_API_SECRET в .env")

print("\n" + "=" * 60)
print("✅ ТЕСТ ЗАВЕРШЕН")
print("=" * 60)