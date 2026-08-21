import asyncio
import aiohttp
import random
import time
import json

API_URL = "http://localhost:8080/api/orders"
USER_COUNT = 10
SYMBOL = "BTC_USDT"

# Gerçekçi fiyat bantları (Satoshi)
BASE_PRICE = 60_000 * 100_000_000
PRICE_VARIANCE = 500 * 100_000_000 # +/- 500$ dalgalanma

# Miktarları daha küçük tutalım (0.01 BTC ile 0.5 BTC arası)
MIN_AMOUNT = int(0.01 * 100_000_000)
MAX_AMOUNT = int(0.5 * 100_000_000)

# Kullanıcıların açık emirlerini takip etmek için bir sözlük (Dictionary)
# Yapısı: { "user_1": ["orderId1", "orderId2"], ... }
active_orders = {f"user_{i}": [] for i in range(1, USER_COUNT + 1)}

async def place_order(session):
    user_id = f"user_{random.randint(1, USER_COUNT)}"
    side = random.choice(["BUY", "SELL"])
    
    # Mevcut fiyattan +/- 500$ sapma ile gerçekçi limit emirler
    price = random.randint(BASE_PRICE - PRICE_VARIANCE, BASE_PRICE + PRICE_VARIANCE)
    amount = random.randint(MIN_AMOUNT, MAX_AMOUNT)

    order_payload = {
        "userId": user_id,
        "symbol": SYMBOL,
        "side": side,
        "type": "LIMIT",
        "price": price,
        "amount": amount
    }

    try:
        async with session.post(API_URL, json=order_payload) as response:
            if response.status == 200:
                data = await response.json()
                order_id = data.get("orderId")
                if order_id:
                    active_orders[user_id].append(order_id)
                print(f"[+] EMIR GIRILDI  | {user_id} | {side} | Fiyat: {price} | Miktar: {amount}")
            else:
                text = await response.text()
                print(f"[!] HATA          | {user_id} - HTTP {response.status} (Yetersiz Bakiye Olabilir)")
    except Exception as e:
        print(f"[x] BAGLANTI HATASI: {e}")

async def cancel_order(session):
    # Rastgele bir kullanıcı seç
    user_id = f"user_{random.randint(1, USER_COUNT)}"
    
    # Kullanıcının aktif emri yoksa iptal edecek bir şey de yoktur
    if not active_orders[user_id]:
        return
        
    # Rastgele bir emrini seçip listeden çıkaralım
    order_id = random.choice(active_orders[user_id])
    active_orders[user_id].remove(order_id)

    cancel_url = f"{API_URL}/{order_id}"
    try:
        async with session.delete(cancel_url) as response:
            if response.status == 200:
                print(f"[-] EMIR IPTAL    | {user_id} | OrderID: {order_id}")
            else:
                print(f"[?] IPTAL BASARISIZ | {user_id} - Zaten eslesmis olabilir. HTTP {response.status}")
    except Exception as e:
        print(f"[x] BAGLANTI HATASI (IPTAL): {e}")

async def main():
    print("=========================================================")
    print(" Kripto Borsasi MARKET MAKER (Piyasa Yapici) Botu ")
    print("=========================================================")
    print("Hem alim/satim yapilacak hem de acik emirler iptal edilecek...")
    
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = []
            # Saniyede 20 yeni sipariş ver (Daha kontrollü bir akış)
            for _ in range(20):
                tasks.append(place_order(session))
                
            # Saniyede 5 tane de rastgele iptal (Cancel) isteği gönder
            for _ in range(5):
                tasks.append(cancel_order(session))
            
            await asyncio.gather(*tasks)
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
