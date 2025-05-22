import requests
import csv
import os
from datetime import datetime, timezone, timedelta
import time

def fetch_historical_ohlcv(network, pool_id, timeframe='day', days=90):
    """
    Fetch historical OHLCV data from GeckoTerminal API
    
    Parameters:
    - network: blockchain network (e.g., 'solana')
    - pool_id: address/ID of the pool
    - timeframe: time interval ('day' for daily data)
    - days: number of days to fetch
    
    Returns:
    - List of OHLCV data points
    """
    url = f"https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool_id}/ohlcv/{timeframe}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        ohlcv_data = response.json()["data"]["attributes"]["ohlcv_list"]
        
        # Calculate cutoff timestamp for filtering (now - days)
        now = datetime.now(timezone.utc)
        cutoff_dt = now - timedelta(days=days)
        cutoff_ts = int(cutoff_dt.timestamp())
        
        # Filter to only include data points newer than cutoff
        filtered_data = [bar for bar in ohlcv_data if bar[0] >= cutoff_ts]
        
        return filtered_data, cutoff_dt, now
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching OHLCV data: {str(e)}")
        return None, None, None
    except (KeyError, ValueError) as e:
        print(f"Error processing OHLCV data: {str(e)}")
        return None, None, None

def fetch_pool_info(network, pool_id):
    """
    Fetch additional pool information including liquidity, volume, etc.
    
    Parameters:
    - network: blockchain network (e.g., 'solana')
    - pool_id: address/ID of the pool
    
    Returns:
    - Dictionary with pool information
    """
    url = f"https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool_id}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        pool_data = response.json()["data"]["attributes"]
        
        base_price_usd = float(pool_data.get("base_token_price_usd") or 0)
        reserve_in_usd = float(pool_data.get("reserve_in_usd") or 0)
        market_cap_usd = float(pool_data.get("market_cap_usd") or 0)

        # Derived in base token units
        reserve_in_base = reserve_in_usd / base_price_usd if base_price_usd else None
        market_cap_base = market_cap_usd / base_price_usd if base_price_usd else None

        info = {
            "name": pool_data.get("name", ""),
            "address": pool_id,
            "base_token_price_usd": base_price_usd,
            "quote_token_price_usd": pool_data.get("quote_token_price_usd"),
            "reserve_in_usd": reserve_in_usd,
            "reserve_in_base": reserve_in_base,
            "market_cap_usd": market_cap_usd,
            "market_cap_base": market_cap_base,
        }
        
        return info
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching pool info: {str(e)}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error processing pool info: {str(e)}")
        return None


def save_to_csv(data, pool_info, filename, cutoff_dt=None, now_dt=None):
    """Save OHLCV and pool info data to CSV file"""
    if data is None or not data:
        print(f"No data to save for {filename}")
        return
    
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        

        # Write header
        writer.writerow([
            "date", "open", "high", "low", "close", "volume",
            "liquidity_usd", "liquidity_base",
            "market_cap_usd", "market_cap_base",
        ])
        
        # Get pool info values and use None if not available
        liquidity_usd = pool_info.get("reserve_in_usd") if pool_info else None
        liquidity_base = pool_info.get("reserve_in_base") if pool_info else None
        market_cap_usd = pool_info.get("market_cap_usd") if pool_info else None
        market_cap_base = pool_info.get("market_cap_base") if pool_info else None
        
        # Write data rows
        for ts, o, h, l, c, v in data:
            dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            writer.writerow([
                dt, o, h, l, c, v,
                liquidity_usd, liquidity_base,
                market_cap_usd, market_cap_base,
            ])
    
    date_range = ""
    if cutoff_dt and now_dt:
        date_range = f" ({len(data)} daily bars, from {cutoff_dt.date()} to {now_dt.date()})"
    
    print(f"✅ Saved {filename}{date_range}")

def main():
    # Create data directory if it doesn't exist
    os.makedirs("data/raw/gecko-terminal", exist_ok=True)

    # Define pools to fetch data for
    pools = [
        {
            "network": "solana",
            "pool_id": "6a3m2EgFFKfsFuQtP4LJJXPcAe3TQYXNyHUjjZpUxYgd",   
            "name": "JLP_SOL",
            "output_csv": "data/raw/gecko-terminal/JLP_SOL_90days.csv"
        },
        {
            "network": "solana",
            "pool_id": "6NUiVmsNjsi4AfsMsEiaezsaV9N4N1ZrD4jEnuWNRvyb",  
            "name": "JLP_USDC",
            "output_csv": "data/raw/gecko-terminal/JLP_USDC_90days.csv"
        },
        {
            "network": "solana",
            "pool_id": "C9U2Ksk6KKWvLEeo5yUQ7Xu46X7NzeBJtd9PBfuXaUSM",   
            "name": "Fartcoin_SOL",
            "output_csv": "data/raw/gecko-terminal/Fartcoin_SOL_90days.csv"
        },
    ]
    
    for pool in pools:
        print(f"Fetching data for {pool['name']}...")
        
        # Fetch OHLCV data
        ohlcv_data, cutoff_dt, now_dt = fetch_historical_ohlcv(
            network=pool['network'],
            pool_id=pool['pool_id'],
            timeframe='day',
            days=90
        )
        
        # Fetch additional pool information
        pool_info = fetch_pool_info(pool['network'], pool['pool_id'])
        
        # Save to CSV
        save_to_csv(
            ohlcv_data, 
            pool_info,
            pool['output_csv'],
            cutoff_dt,
            now_dt
        )
        
        # Add delay between requests to avoid rate limiting
        time.sleep(1)

if __name__ == "__main__":
    main()