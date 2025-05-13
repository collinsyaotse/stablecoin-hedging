import os
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def fetch_messari():
    """Fetch 2 years of historical USDC price metrics from Messari and save to CSV."""
    url = 'https://data.messari.io/api/v1/assets/usdc/metrics/price/time-series'

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=2 * 365)

    params = {
        'start': start_date.strftime('%Y-%m-%d'),
        'end': end_date.strftime('%Y-%m-%d'),
        'interval': '1d',
        'format': 'json'
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        print("Status Code:", response.status_code)

        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch data, status code: {response.status_code}")

        data = response.json()
        values = data.get("data", {}).get("values")

        if not values:
            raise KeyError("No 'values' found in the response JSON.")

        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(values, columns=columns)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        os.makedirs('data/raw/messari', exist_ok=True)
        df.to_csv('data/raw/messari/messari_usdc_price_timeseries.csv', index=False)
        print("✅ Saved historical USDC data to data/raw/messari/messari_usdc_price_timeseries.csv")

    except requests.RequestException as err:
        raise RuntimeError(f"HTTP request error from Messari: {err}") from err
    except ValueError as err:
        raise ValueError(f"JSON decoding error: {err}") from err
    except KeyError as err:
        raise KeyError(f"Missing expected data structure: {err}") from err

if __name__ == "__main__":
    fetch_messari()

