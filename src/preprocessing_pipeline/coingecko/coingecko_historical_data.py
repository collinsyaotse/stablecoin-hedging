"""
Coingecko Stablecoin Preprocessing Script

This script loads, cleans, and processes Coingecko historical price data
for DAI, Tether, and USDC. It adds rolling statistics, normalizes values,
and saves the cleaned version for each coin.
"""

import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def preprocess_coin_file(input_path, output_path, window_size=7):
    '''Preprocess a single Coingecko historical stablecoin data file.'''
    
    # Load CSV
    df = pd.read_csv(input_path)

    # Convert timestamp to datetime and sort
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    # Forward fill missing values
    df = df.ffill()

    # Fill any NaNs introduced by rolling
    df = df.fillna(method='bfill')

    # Normalize selected columns
    scaler = MinMaxScaler()
    df[['price', 'market_cap', 'volume']] = scaler.fit_transform(
        df[['price', 'market_cap', 'volume']]
    )

    # Save cleaned file
    df.to_csv(output_path, index=False)
    print(f"Processed file saved to: {output_path}")



if __name__ == "__main__":
    INPUT_DIR = 'data/raw/coingecko'
    OUTPUT_DIR = 'data/processed/coingecko'
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stablecoins = ['dai', 'tether', 'usd-coin']
    
    for coin in stablecoins:
        in_file = os.path.join(INPUT_DIR, f"{coin}_historical_data.csv")
        out_file = os.path.join(OUTPUT_DIR, f"cleaned_{coin}_data.csv")
        preprocess_coin_file(in_file, out_file)
