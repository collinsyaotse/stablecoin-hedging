"""
Module to fetch economic indicators using yFinance and store them as CSV files.
"""

import os
from dotenv import load_dotenv
import yfinance as yf

# Load environment variables
load_dotenv()

# Define constants
DATA_DIR = "data/raw/yfinance"
TICKERS = {
    "yields": "^TNX",
    "interest_rates": "^IRX",
    "fx_rates": "EURUSD=X",
    "volatility_indices": "^VIX"
}

def fetch_and_save_yfinance_data():
    """Fetch economic indicators using yFinance and save them to CSV files."""
    os.makedirs(DATA_DIR, exist_ok=True)

    for label, ticker in TICKERS.items():
        try:
            print(f"Fetching {label.replace('_', ' ').title()} ({ticker})...")
            data = yf.download(ticker, period="2y")
            if data is None or data.empty:
                raise ValueError(f"No data returned for ticker: {ticker}")

            filepath = os.path.join(DATA_DIR, f"{label}.csv")
            data.to_csv(filepath)
            print(f"{label.replace('_', ' ').title()} saved to {filepath}")

        except Exception as err:
            raise RuntimeError(f"Failed to fetch or save {label}: {err}") from err

if __name__ == "__main__":
    fetch_and_save_yfinance_data()
