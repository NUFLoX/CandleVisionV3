from api.bybit_client import BybitClient

# Создаем клиент для TESTNET
client = BybitClient(testnet=True)

# Если session создан → ключи работают
if client.session:
    print("✅ Session создан успешно!")
    
    # Попытка получить баланс
    try:
        balance = client.session.get_wallet_balance(accountType="UNIFIED")
        print(f"✅ Баланс получен: {balance}")
    except Exception as e:
        print(f"❌ Ошибка получения баланса: {e}")
else:
    print("❌ Session не инициализирован")