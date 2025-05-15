"""
USDC Historical Data Preprocessing Script (Messari)
This script loads, cleans, and preprocesses a single CSV of USDC historical data.
It adds daily return, volatility, log return, normalizes price/volume columns,
and outputs a cleaned version to a CSV file.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import os  # <- Add this to use os.makedirs

def preprocess_usdc_data(input_file, output_file):
    '''Load, clean, and preprocess USDC historical data from Messari.'''

    # Load CSV
    df = pd.read_csv(input_file)

    # Convert timestamp to datetime and sort
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    # Forward fill missing values
    df = df.ffill()

    # Filter out invalid rows
    df = df[(df['low'] <= df[['open', 'close']].min(axis=1)) & 
            (df['high'] >= df[['open', 'close']].max(axis=1)) & 
            (df['volume'] >= 0)]

    # Add calculated columns
    df['daily_return'] = (df['close'] - df['open']) / df['open']
    df['volatility'] = (df['high'] - df['low']) / df['open']
    df['log_return'] = np.log(df['close'] / df['open'])

    # Normalize selected columns
    scaler = MinMaxScaler()
    df[['open', 'high', 'low', 'close', 'volume']] = scaler.fit_transform(
        df[['open', 'high', 'low', 'close', 'volume']]
    )

    # Ensure output directory exists before saving
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Save cleaned dataframe
    df.to_csv(output_file, index=False)
    print(f"Cleaned USDC data saved to: {output_file}")

# Define input/output paths
INPUT_FILE = 'data/raw/messari/usdc_historical_data.csv'
OUTPUT_FILE = 'data/processed/messari/cleaned_usdc_data.csv'

# Run the preprocessing
preprocess_usdc_data(INPUT_FILE, OUTPUT_FILE)
