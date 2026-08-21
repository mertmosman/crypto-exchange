import requests

API_URL = "http://localhost:8080/api/wallets/seed"
# Her kullanıcıya 1.000.000 USD (1 Milyon $) ve 100 BTC (Satoshi cinsinden)
USDT_AMOUNT = 1_000_000 * 100_000_000
BTC_AMOUNT = 100 * 100_000_000

for i in range(1, 11):
    user_id = f"user_{i}"
    url = f"{API_URL}?userId={user_id}&usdtAmount={USDT_AMOUNT}&btcAmount={BTC_AMOUNT}"
    response = requests.post(url)
    if response.status_code == 200:
        print(f"[{user_id}] basariyla olusturuldu ve bakiye eklendi.")
    else:
        print(f"[{user_id}] HATA: {response.text}")
