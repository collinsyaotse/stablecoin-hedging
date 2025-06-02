import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

# Pool addresses for the two trading pairs
JLP_USDC_POOL = "6NUiVmsNjsi4AfsMsEiaezsaV9N4N1ZrD4jEnuWNRvyb"  # Orca
JLP_SOL_POOL = "J2Gsg3xTDjM8UZjKdEqzBeDwitHb2Ux6TSBX5RzsE42r"   # Meteora

# GeckoTerminal API base URL
BASE_URL = "https://api.geckoterminal.com/api/v2"

# Current date and 6 months ago
current_date = datetime.now()
six_months_ago = current_date - timedelta(days=180)

print(f"Fetching historical data from {six_months_ago.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")
print(f"JLP/USDC Pool: {JLP_USDC_POOL}")
print(f"JLP/SOL Pool: {JLP_SOL_POOL}")

def fetch_pool_ohlcv(pool_address, timeframe="day", aggregate=1, limit=1000):
    """
    Fetch OHLCV data from GeckoTerminal API
    """
    url = f"{BASE_URL}/networks/solana/pools/{pool_address}/ohlcv/{timeframe}"
    params = {
        "aggregate": aggregate,
        "limit": limit
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract OHLCV data
        ohlcv_list = data['data']['attributes']['ohlcv_list']
        
        # Convert to DataFrame
        df = pd.DataFrame(ohlcv_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Convert timestamp to datetime
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('datetime', inplace=True)
        
        # Convert volume to float
        df['volume'] = df['volume'].astype(float)
        
        #save to csv
        output_file = f"{pool_address}_ohlcv_data.csv"
        df.to_csv(output_file)
        print(f"✅ OHLCV data for pool {pool_address} saved to {output_file}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for pool {pool_address}: {e}")
        return None
    except Exception as e:
        print(f"Error processing data for pool {pool_address}: {e}")
        return None

# Test API access first
print("\nTesting API access...")
test_response = requests.get(f"{BASE_URL}/networks/solana/pools/{JLP_USDC_POOL}")
print(f"API Status: {test_response.status_code}")

if test_response.status_code == 200:
    print("API access successful!")
else:
    print(f"API access failed: {test_response.text}")
    
# Fetch and save OHLCV data for both pools
jlp_usdc_df = fetch_pool_ohlcv(JLP_USDC_POOL)
jlp_sol_df = fetch_pool_ohlcv(JLP_SOL_POOL)