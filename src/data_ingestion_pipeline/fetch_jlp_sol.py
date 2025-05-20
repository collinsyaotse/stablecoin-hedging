import requests

API_KEY = "9e4d427f-da3b-4206-8400-5ac443a48613"

headers = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": API_KEY,
}

def get_coin_id(symbol):
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/map?symbol={symbol}"
    response = requests.get(url, headers=headers)
    data = response.json()
    if data["status"]["error_code"] == 0 and data["data"]:
        return data["data"][0]["id"]
    else:
        raise ValueError("Token not found.")

def get_coin_quote(coin_id):
    url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?id={coin_id}"
    response = requests.get(url, headers=headers)
    return response.json()

# ---- Run the code ----
symbol = "JLP"  
try:
    coin_id = get_coin_id(symbol)
    data = get_coin_quote(coin_id)
    quote = data["data"][str(coin_id)]["quote"]["USD"]

    print(f"Price: ${quote['price']:.4f}")
    print(f"24h Volume: ${quote['volume_24h']:.2f}")
    print(f"Market Cap: ${quote['market_cap']:.2f}")
    print(f"Percent Change 24h: {quote['percent_change_24h']:.2f}%")
    print(f"Bid/Ask Spread: N/A (not directly available)")
except Exception as e:
    print("Error:", e)
