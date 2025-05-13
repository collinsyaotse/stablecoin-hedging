import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def clean_fx_rates(file_path):
    """
    Cleans a YFinance-style FX CSV with 3 header rows,
    where the real data starts on line 4.

    Returns a fully preprocessed DataFrame.
    """
    # 1. Read in, skipping the first 3 non-data rows
    df = pd.read_csv(
        file_path,
        skiprows=3,
        names=['timestamp','Close','High','Low','Open','Volume'],
        dtype={'Volume': float}
    )

    # 2. Parse timestamps with explicit format YYYY-MM-DD
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d')

    # 3. Sort and drop exact duplicates
    df = df.sort_values('timestamp').drop_duplicates(subset=['timestamp'])

    #Drop Volume
    df = df.drop(columns=['Volume'])

    # 4. Forward-fill any missing data
    df = df.ffill()

    # 5. Ensure numeric types
    for col in ['Open','High','Low','Close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 6. Derived features
    df['daily_return'] = (df['Close'] - df['Open']) / df['Open']
    df['volatility']   = (df['High']  - df['Low'])  / df['Open']
    df['log_return']   = np.log(df['Close'] / df['Open'])

    # 8. Normalize price & volume to [0,1]
    scaler = MinMaxScaler()
    df[['Open','High','Low','Close']] = scaler.fit_transform(
        df[['Open','High','Low','Close']]
    )

    # 9. Optional: reset index if you want a flat DataFrame
    df.reset_index(drop=True, inplace=True)

    return df

# Usage:
cleaned_fx = clean_fx_rates('data/raw/yfinance/fx_rates.csv')
cleaned_fx.to_csv('data/processed/cleaned_fx_rates.csv', index=False)
print("Data saved successfully.")
