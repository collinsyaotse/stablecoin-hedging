import requests
import csv
from datetime import datetime, timedelta, timezone

# ✅ Confirm this address directly from Birdeye!
FARTCOIN_MINT = "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump"
API_KEY = "2d50cec250624eb0944a82e3ca55082d"

# Use timezone-aware UTC
end_time = int(datetime.now(timezone.utc).timestamp())
start_time = int((datetime.now(timezone.utc) - timedelta(days=730)).timestamp())

url = "https://public-api.birdeye.so/public/price/history"

params = {
    "address": FARTCOIN_MINT,
    "time_from": start_time,
    "time_to": end_time,
    "resolution": "1d"
}

headers = {
    "X-API-KEY": API_KEY
}

response = requests.get(url, params=params, headers=headers)

if response.status_code == 200:
    prices = response.json()['data']['items']
    with open("fartcoin_sol_history.csv", mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "price_usd"])
        for item in prices:
            ts = datetime.utcfromtimestamp(item['timestamp']).strftime('%Y-%m-%d')
            writer.writerow([ts, item['value']])
    print("✅ Saved 2-year price history to fartcoin_sol_history.csv")
else:
    print(f"❌ Error: {response.status_code} - {response.text}")
