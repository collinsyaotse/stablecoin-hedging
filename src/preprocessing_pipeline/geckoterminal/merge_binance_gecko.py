import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def preprocess_binance(input_dir):
    df_dict = {}

    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_dir, file_name)
            df = pd.read_csv(file_path)

            # Standardize date column
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            # Fill missing values and remove duplicates
            df = df.ffill()
            df = df[~df.index.duplicated(keep='first')]

            # Filter invalid rows
            df = df[(df['low'] <= df[['open', 'close']].min(axis=1)) & 
                    (df['high'] >= df[['open', 'close']].max(axis=1)) & 
                    (df['volume'] >= 0)]

            # Add derived columns
            df['daily_return'] = (df['close'] - df['open']) / df['open']
            df['volatility'] = (df['high'] - df['low']) / df['open']
            df['log_return'] = np.log(df['close'] / df['open'])

            # Add pair column
            pair_name = file_name.replace("df_", "").replace("_historical_data.csv", "")
            df['pair'] = pair_name

            # Normalize numeric columns
            scaler = MinMaxScaler()
            df[['open', 'high', 'low', 'close', 'volume']] = scaler.fit_transform(
                df[['open', 'high', 'low', 'close', 'volume']]
            )

            df_dict[file_name] = df

    merged_binance = pd.concat(df_dict.values(), axis=0).reset_index(drop=True)
    return merged_binance


def preprocess_gecko(file_path):
    df = pd.read_csv(file_path)

    # Standardize timestamp column
    df['timestamp'] = pd.to_datetime(df['date'])
    df.drop(columns=['date'], inplace=True)

    # Fill missing values with 0
    df = df.fillna(0)

    # Rename 'source' to 'pair'
    if 'source' in df.columns:
        df.rename(columns={'source': 'pair'}, inplace=True)

    # Ensure derived columns exist
    if all(col in df.columns for col in ['open', 'close', 'high', 'low']):
        df['daily_return'] = (df['close'] - df['open']) / df['open']
        df['volatility'] = (df['high'] - df['low']) / df['open']
        df['log_return'] = np.log(df['close'] / df['open'])

    # Normalize numeric columns
    to_scale = ['open', 'high', 'low', 'close']
    for col in to_scale:
        if col in df.columns:
            scaler = MinMaxScaler()
            df[col] = scaler.fit_transform(df[[col]])

    return df


def merge_binance_gecko(binance_dir, gecko_file, output_file):
    binance_df = preprocess_binance(binance_dir)
    gecko_df = preprocess_gecko(gecko_file)

    # Ensure common columns before merging
    common_cols = binance_df.columns.intersection(gecko_df.columns).tolist()

    merged_df = pd.concat(
        [binance_df[common_cols], gecko_df[common_cols]],
        axis=0
    ).sort_values(['pair', 'timestamp']).reset_index(drop=True)

    # move 'pair' col next to 'timestamp'
    cols = merged_df.columns.tolist()
    if 'pair' in cols and 'timestamp' in cols:
        cols.remove('pair')
        pair_index = cols.index('timestamp') + 1
        cols.insert(pair_index, 'pair')
        merged_df = merged_df[cols]


    merged_df.to_csv(output_file, index=False)
    print(f"✅ Final merged dataset saved to: {output_file}")


# Paths
BINANCE_DIR = 'data/raw/binance/historical-data'
GECKO_FILE = 'data/processed/geckoterminal/MERGED_POOLS_90days.csv'
MERGED_OUTPUT = 'data/processed/complete_merge/merged_binance_gecko.csv'

# Run
merge_binance_gecko(BINANCE_DIR, GECKO_FILE, MERGED_OUTPUT)
