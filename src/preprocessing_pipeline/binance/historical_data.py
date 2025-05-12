'''
Funding Rates Preprocessing Script
This script merges and cleans funding rate data
from multiple CSV files.
It handles missing values, normalizes the rate column, 
and saves the cleaned data to a new CSV file.
'''

import os
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd

def clean_preprocess(input_dir, output_file):
    '''Merge and clean historical data from multiple CSV files.'''
    df_dict_historical = {}

    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_dir, file_name)
            df = pd.read_csv(file_path)
            # Convert timestamp to datetime and sort
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            df.set_index('timestamp', inplace=True)

            # Fill missing values and remove duplicates
            df = df.ffill()
            df = df[~df.index.duplicated(keep='first')]

            # Filter invalid rows
            df = df[(df['low'] <= df[['open', 'close']].min(axis=1)) & 
                (df['high'] >= df[['open', 'close']].max(axis=1)) & 
                (df['volume'] >= 0)]

            # Add calculated columns
            df['daily_return'] = (df['close'] - df['open']) / df['open']
            df['volatility'] = (df['high'] - df['low']) / df['open']
            df['log_return'] = np.log(df['close'] / df['open'])

            # Add 'pair' column
            pair_name = file_name.replace("df_", "")
            pair_name = pair_name.replace("_historical_data.csv", "")
            df['pair'] = pair_name

            # Normalize selected columns
            scaler = MinMaxScaler()
            df[['open', 'high', 'low', 'close', 'volume']] = scaler.fit_transform(
                df[['open', 'high', 'low', 'close', 'volume']]
            )

            df_dict_historical[file_name] = df


# Merge all dataframes vertically
    merged_historical_df = pd.concat(df_dict_historical.values(), axis=0)

    # Optional: Reset index if you don't want timestamps as the index
    merged_historical_df.reset_index(drop=True, inplace=True)

    # Save merged version
    merged_historical_df.to_csv(output_file, index=False)
    print(f"Merged historical data saved to {output_file}")

# Define input directory and output file
INPUT_DIRECTORY = 'data/raw/binance/historical-data'
OUTPUT_CSV = 'data/processed/binance/merged_historical_data.csv'

# Run the function
clean_preprocess(INPUT_DIRECTORY, OUTPUT_CSV)