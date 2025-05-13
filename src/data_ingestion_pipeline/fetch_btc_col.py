import requests
params = {
    "vs_currency": "usd",
    "ids": "tether,usd-coin,dai",
}
r = requests.get("https://api.coingecko.com/api/v3/coins/markets", params=params)
print(r.json())
