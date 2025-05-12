from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import numpy as np
import os

def clean_liquidity_data(input_dir, output_file):
    df_dict_liquidity = {}
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_dir, file_name)
            df = pd.read_csv(file_path)
            # Parse timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            df.set_index('timestamp', inplace=True)

            # Forward fill missing values
            df = df.ffill()

            # Remove duplicate timestamps
            df = df[~df.index.duplicated(keep='first')]

            # Logical validation
            df = df[
                (df['low'] <= df[['open', 'close']].min(axis=1)) &
                (df['high'] >= df[['open', 'close']].max(axis=1)) &
                (df['volume'] >= 0) &
                (df['bid'] <= df['ask'])
            ]

            # Derived features
            df['daily_return'] = (df['close'] - df['open']) / df['open']
            df['volatility'] = (df['high'] - df['low']) / df['open']
            df['log_return'] = np.log(df['close'] / df['open'])

            # Recalculate spread (in case of slight float issues)
            df['spread'] = df['ask'] - df['bid']

            # Normalize selected numeric columns
            scaler = MinMaxScaler()
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'bid', 'ask', 'spread']
            df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

            df_dict_liquidity[file_name] = df
    # Merge all dataframes vertically
    merged_liquidity_df = pd.concat(df_dict_liquidity.values(), axis=0)

    # Optional: Reset index if you don't want timestamps as the index
    merged_liquidity_df.reset_index(drop=True, inplace=True)

    # Save merged version
    merged_liquidity_df.to_csv(output_file, index=False)
    print(f"Merged liquidity data saved to {output_file}")

# Define input directory and output file
input_directory = 'data/raw/binance/trading-volume'
output_csv = 'data/processed/binance/merged_trading_volume_liquidity_data.csv'

# Run the function
clean_liquidity_data(input_directory, output_csv)
