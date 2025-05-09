import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
import ccxt
import pandas as pd



# Load environment variables from .env file
load_dotenv()


def fetch_historical_data(binance, pair, since):
    """Fetch historical OHLCV data for a given pair since a specific timestamp."""
    try:
        ohlcv = binance.fetch_ohlcv(pair, timeframe='1d', since=since)
        if ohlcv:
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 
                                              'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            csv_filename = f"data/raw/{pair.replace('/', '_')}_historical_data.csv"
            os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
            df.to_csv(csv_filename, index=False)
            print(f"Historical data for {pair} saved to {csv_filename}")
        else:
            print(f"No historical data fetched for {pair}")
    except ccxt.BaseError as err:
        print(f"CCXT BaseError occurred while fetching historical data for {pair}: {err}")
    except Exception as err:
        print(f"Unexpected error fetching historical data for {pair}: {err}")

# Initialize Binance API
binance_api_key = os.getenv("BINANCE_API_KEY")
binance_api_secret = os.getenv("BINANCE_API_SECRET")
binance = ccxt.binance({
    'apiKey': binance_api_key,
    'secret': binance_api_secret,
})

# Define pairs and fetch historical data for the last 2 years
binance_coin_pairs = [
    "SOL/USDC",
    "ETH/USDT",
    "BNB/USDT",
    "BTC/USDC",
    "ETH/USDC",
    "ETH/DAI",
    "BTC/DAI",
    "LTC/USDT"
]

def fetch_trading_volume_and_liquidity(binance, pair):
    """Fetch trading volume and liquidity metrics for a given pair."""
    try:
        ticker = binance.fetch_ticker(pair)
        volume = ticker.get('quoteVolume', 0)
        bid = ticker.get('bid', 0)
        ask = ticker.get('ask', 0)
        spread = ask - bid if ask and bid else None
        csv_filename = f"data/raw/{pair.replace('/', '_')}_trading_volume_liquidity.csv"
        os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
        df = pd.DataFrame([{
            'pair': pair,
            'volume': volume,
            'bid': bid,
            'ask': ask,
            'spread': spread
        }])
        df.to_csv(csv_filename, index=False)
        print(f"Trading volume and liquidity data for {pair} saved to {csv_filename}")
        print(f"Trading volume for {pair}: {volume}")
        print(f"Liquidity metrics for {pair} - Bid: {bid}, Ask: {ask}, Spread: {spread}")
    except ccxt.BaseError as err:
        print(f"CCXT BaseError occurred while fetching trading volume and liquidity for {pair}: {err}")
    except Exception as err:
        print(f"Unexpected error fetching trading volume and liquidity for {pair}: {err}")

def fetch_funding_rates(binance, pair):
    """Fetch funding rates for perpetual contracts for a given pair."""
    try:
        funding_rate = binance.fetch_funding_rate(pair)
        csv_filename = f"data/raw/{pair.replace('/', '_')}_funding_rates.csv"
        os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
        df = pd.DataFrame([funding_rate])
        df.to_csv(csv_filename, index=False)
        print(f"Funding rate for {pair} saved to {csv_filename}")
    except ccxt.BaseError as err:
        print(f"CCXT BaseError occurred while fetching funding rates for {pair}: {err}")
    except Exception as err:
        print(f"Unexpected error fetching funding rates for {pair}: {err}")

    # Modify fetch_trading_volume_and_liquidity to fetch data for a 2-year span
    def fetch_trading_volume_and_liquidity(binance, pair, since):
        """Fetch trading volume and liquidity metrics for a given pair over a 2-year span."""
        try:
            ohlcv = binance.fetch_ohlcv(pair, timeframe='1d', since=since)
            if ohlcv:
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 
                                  'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['pair'] = pair
                df['bid'] = None
                df['ask'] = None
                df['spread'] = None

                for index, row in df.iterrows():
                    ticker = binance.fetch_ticker(pair)
                    df.at[index, 'bid'] = ticker.get('bid', 0)
                    df.at[index, 'ask'] = ticker.get('ask', 0)
                    df.at[index, 'spread'] = df.at[index, 'ask'] - df.at[index, 'bid'] if df.at[index, 'ask'] and df.at[index, 'bid'] else None

                csv_filename = f"data/raw/{pair.replace('/', '_')}_trading_volume_liquidity_2_years.csv"
                os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
                df.to_csv(csv_filename, index=False)
                print(f"Trading volume and liquidity data for {pair} over 2 years saved to {csv_filename}")
            else:
                print(f"No trading volume and liquidity data fetched for {pair}")
        except ccxt.BaseError as err:
            print(f"CCXT BaseError occurred while fetching trading volume and liquidity for {pair}: {err}")
        except Exception as err:
            print(f"Unexpected error fetching trading volume and liquidity for {pair}: {err}")
        except ccxt.BaseError as err:
            print(f"CCXT BaseError occurred while fetching trading volume and liquidity for {pair}: {err}")
        except Exception as err:
            print(f"Unexpected error fetching trading volume and liquidity for {pair}: {err}")
since_timestamp = int((datetime.now() - timedelta(days=730)).timestamp() * 1000)

for pair in binance_coin_pairs:
    fetch_historical_data(binance, pair, since_timestamp)
    fetch_trading_volume_and_liquidity(binance, pair)
    fetch_funding_rates(binance, pair)

print("Historical data fetching completed.")

if __name__ == "__main__":
    pass