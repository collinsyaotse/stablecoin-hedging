"""Fetch and save historical price, market cap, and volume data for stablecoins from CoinGecko."""

import os
import pandas as pd
from pycoingecko import CoinGeckoAPI

def fetch_coingecko_data():
    """Fetch and save data metrics for Tether, USDC, and DAI."""
    cg = CoinGeckoAPI()
    coins = ["tether", "usd-coin", "dai"]
    base_currency = 'usd'
    days = 365
    output_dir = os.path.join(os.getcwd(), 'data', 'raw', 'coingecko')
    os.makedirs(output_dir, exist_ok=True)

    for coin in coins:
        try:
            data = cg.get_coin_market_chart_by_id(coin, base_currency, days)

            # Create DataFrames
            prices_df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            market_caps_df = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
            volumes_df = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])

            # Merge and process data
            df = prices_df.merge(market_caps_df, on='timestamp').merge(volumes_df, on='timestamp')
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Compute deviation metrics
            df['rolling_mean'] = df['price'].rolling(window=7).mean()
            df['price_deviation'] = df['price'] - df['rolling_mean']
            df['price_std'] = df['price'].rolling(window=7).std()

            # Save as CSV
            output_file = os.path.join(output_dir, f"{coin}_full_data.csv")
            df.to_csv(output_file, index=False)
            print(f"✅ Full data for {coin} saved to {output_file}")

        except Exception as err:  # Use specific variable name
            print(f"❌ Failed to fetch/save data for {coin}: {err}")

if __name__ == "__main__":
    fetch_coingecko_data()
