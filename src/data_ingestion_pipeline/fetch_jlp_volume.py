import requests
import datetime
import time
import pandas as pd
from dotenv import load_dotenv
import os


def fetch_data(base):
    # Load API key
    load_dotenv()
    API_KEY = os.getenv("COINGECKO_API_KEY")

    COIN_ID = "jupiter-perpetuals-liquidity-provider-token"
    BASE_URL = "https://api.coingecko.com/api/v3/coins"
    HEADERS = {
        "accept": "application/json",
        "x-cg-demo-api-key": API_KEY
    }
    data = []
    start_date = datetime.date.today() - datetime.timedelta(days=350)

    for i in range(350):
        date = start_date + datetime.timedelta(days=i)
        formatted_date = date.strftime("%d-%m-%Y")
        url = f"{BASE_URL}/{COIN_ID}/history?date={formatted_date}"

        success = False
        retries = 0

        while not success and retries < 3:
            try:
                response = requests.get(url, headers=HEADERS)
                if response.status_code == 200:
                    result = response.json()
                    market_data = result.get("market_data", {})
                    price = market_data.get("current_price", {}).get(f"{base}", None)
                    volume = market_data.get("total_volume", {}).get(f"{base}", None)
                    market_cap = market_data.get("market_cap", {}).get(f"{base}", None)
                    data.append({
                        "date": date,
                        f"price_{base}": price,
                        f"volume_{base}": volume,
                        f"market_cap_{base}": market_cap
                    })
                    print(f"[{formatted_date}] ✅ Success")
                    success = True
                elif response.status_code == 429:
                    wait_time = 5 * (retries + 1)
                    print(f"[{formatted_date}] ⏳ Rate limited (429). Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    print(f"[{formatted_date}] ❌ Failed: {response.status_code}")
                    break
            except Exception as e:
                print(f"❌ Error on {formatted_date}: {str(e)}")
                break

        time.sleep(3.5)  # Respectful delay between requests

    # Save results
    df = pd.DataFrame(data)
    df.to_csv("data/raw/coingecko/jlp_sol_350_days_data.csv", index=False)
    print("✅ Data saved to 'jlp_350_days_data.csv'")

if __name__ == "__main__":
    # Example usage  
    base_usd = "usd"
    fetch_data(base_usd)
