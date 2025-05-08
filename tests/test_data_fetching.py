"""
Unit tests for data fetching functionality from various APIs and services
used in the stablecoin-hedging project.
"""

import os
import unittest

from dotenv import load_dotenv
import requests
import yfinance as yf
import ccxt
from pycoingecko import CoinGeckoAPI
from web3 import Web3

# Load environment variables from .env file
load_dotenv()


class TestDataFetching(unittest.TestCase):
    """
    Test suite for validating data fetching functionality from various APIs
    and services used in the stablecoin-hedging project.
    """

    def test_fetch_ccxt(self):
        """Test the Binance API fetch function for expected data format."""
        binance_api_key = os.getenv("BINANCE_API_KEY")
        binance_api_secret = os.getenv("BINANCE_API_SECRET")
        try:
            binance = ccxt.binance({
                'apiKey': binance_api_key,
                'secret': binance_api_secret,
            })
            ticker = binance.fetch_ticker('BNB/USDT')
            self.assertIsNotNone(ticker)
            self.assertIn('last', ticker)
            print("CCXT Binance Data:", ticker)
        except ccxt.BaseError as err:
            self.fail(f"CCXT BaseError occurred: {err}")
        except Exception as err:
            self.fail(f"Unexpected error fetching Binance data: {err}")

    def test_fetch_coingecko(self):
        """Test the CoinGecko API fetch function for expected data format."""
        cg = CoinGeckoAPI()
        try:
            usdt_data = cg.get_coin_market_chart_by_id('tether', 'usd', 365)
            self.assertIsNotNone(usdt_data)
            self.assertIn('prices', usdt_data)
            print("CoinGecko Data:", usdt_data)
        except Exception as err:
            self.fail(f"Fetching data from CoinGecko failed: {err}")

    def test_fetch_web3(self):
        """Test the Web3 API fetch function for expected data format."""
        infura_url = os.getenv("INFURA_HTTPS_URL")
        w3 = Web3(Web3.HTTPProvider(infura_url))
        if w3.is_connected():
            try:
                eth_block = w3.eth.get_block('latest')
                self.assertIsNotNone(eth_block)
                self.assertIn('number', eth_block)
                print("Ethereum Data (Latest Block):", eth_block)
            except Exception as err:
                self.fail(f"Fetching block data via Web3 failed: {err}")
        else:
            self.fail("Failed to connect to Ethereum via Web3")

    def test_fetch_messari(self):
        """Test the Messari API fetch function for expected data format."""
        url = 'https://data.messari.io/api/v1/assets/usdc/metrics'
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            self.assertIsNotNone(data)
            self.assertIn('data', data)
            print("Messari Data:", data)
        except requests.RequestException as err:
            self.fail(f"HTTP request error from Messari: {err}")
        except ValueError as err:
            self.fail(f"JSON decoding error from Messari: {err}")

    def test_fetch_yfinance(self):
        """Test the yFinance API fetch function for expected data format."""
        try:
            yields = yf.download('^TNX', period='1y')
            self.assertIsNotNone(yields)
            self.assertGreater(len(yields), 0)
            print("yFinance Data (Treasury Yields):", yields)
        except Exception as err:
            self.fail(f"Fetching data from yFinance failed: {err}")


if __name__ == '__main__':
    unittest.main()
