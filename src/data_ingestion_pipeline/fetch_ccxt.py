import os
from datetime import datetime, timedelta
import time

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

# Initialize Binance Futures API
binance_futures = ccxt.binance({
    'apiKey': binance_api_key,
    'secret': binance_api_secret,
    'options': {
        'defaultType': 'future'  # <-- this is key
    }
})

binance_futures.fetch_funding_rate('BNB/USDT:USDT')

# Define pairs and fetch historical data for the last 2 years
binance_coin_pairs = [
    "Fartcoin/SOL",
    "ETH/USDT",
    "BNB/USDT",
    "BTC/USDC",
    "ETH/USDC",
    "ETH/DAI",
    "BTC/DAI",
    "LTC/USDT"
]

futures_pairs = [
    "BNB/USDT:USDT",
    "BTC/USDT:USDT",
    "ETH/USDT:USDT"
]

funding_rate_pairs = [
 
    "SOL/USDC:USDC",
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

def fetch_funding_rates(exchange, pair):
    try:
        funding_rate = exchange.fetch_funding_rate(pair)
        csv_filename = f"data/raw/{pair.replace('/', '_')}_funding_rates.csv"
        os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
        df = pd.DataFrame([funding_rate])
        funding_rate['timestamp'] = pd.to_datetime(funding_rate['timestamp'], unit='ms')
        funding_rate['pair'] = pair
        funding_rate['rate'] = funding_rate.get('fundingRate', None)
        df = pd.DataFrame([{
            'timestamp': funding_rate['timestamp'],
            'pair': funding_rate['pair'],
            'rate': funding_rate['rate']
        }])
        df.to_csv(csv_filename, index=False)
        print(f"Funding rate for {pair} saved to {csv_filename}")
    except ccxt.BaseError as err:
        print(f"CCXT BaseError occurred while fetching funding rates for {pair}: {err}")
    except Exception as err:
        print(f"Unexpected error fetching funding rates for {pair}: {err}")

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
exchange = ccxt.binance({'enableRateLimit': True}, )

def fetch_historical_funding_rates_with_retry(pair, since_days=730, max_retries=5, save_dir='data/raw'):
    # Create the exchange with increased timeout and rate limit enabled
    binance_futures = ccxt.binanceusdm({
        'enableRateLimit': True,
        'timeout': 60000  # 60 seconds
    })

    try:
        # Load market metadata (fetches exchange info)
        binance_futures.load_markets()
    except Exception as e:
        print(f"Failed to load Binance markets: {e}")
        return

    since = binance_futures.parse8601((datetime.utcnow() - timedelta(days=since_days)).isoformat())
    all_rates = []

    print(f"Fetching funding rate history for {pair}...")

    while True:
        for attempt in range(max_retries):
            try:
                rates = binance_futures.fetch_funding_rate_history(pair, since=since, limit=1000)
                break  # exit retry loop on success
            except ccxt.RequestTimeout:
                print(f"RequestTimeout for {pair} — retrying ({attempt+1}/{max_retries}) in 5s...")
                time.sleep(5)
            except Exception as e:
                print(f"Unexpected error for {pair}: {e}")
                return  # give up on other errors
        else:
            print(f"Failed to fetch funding rates for {pair} after {max_retries} retries.")
            return

        if not rates:
            break

        all_rates.extend(rates)
        since = rates[-1]['timestamp'] + 1  # advance since timestamp

        # Optional: throttle requests (good practice)
        time.sleep(binance_futures.rateLimit / 1000)

    if not all_rates:
        print(f"No funding rate data retrieved for {pair}.")
        return

    # Format results into DataFrame
    df = pd.DataFrame(all_rates)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['pair'] = pair
    df['rate'] = df.get('fundingRate', None)
    df = df[['timestamp', 'pair', 'rate']]

    # Save to CSV
    os.makedirs(save_dir, exist_ok=True)
    filename = os.path.join(save_dir, f"{pair.replace('/', '_').replace(':', '_')}_funding_rates_2y.csv")
    df.to_csv(filename, index=False)
    print(f"✅ Saved 2 years of funding rates for {pair} to {filename}")

    return df



for pairs in funding_rate_pairs:
    fetch_historical_funding_rates_with_retry(binance_futures, pairs)
print("Funding rates fetching completed.")

if __name__ == "__main__":
    pass


