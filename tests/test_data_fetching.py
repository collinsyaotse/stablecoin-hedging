import unittest
import ccxt
import yfinance as yf
import requests
from pycoingecko import CoinGeckoAPI
from web3 import Web3
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class TestDataFetching(unittest.TestCase):

    # Test fetching data from Binance via CCXT
    def test_fetch_ccxt(self):
        binance_api_key = os.getenv('BINANCE_API_KEY')
        binance_api_secret = os.getenv('BINANCE_API_SECRET')
        
        binance = ccxt.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
        })
        
        try:
            ticker = binance.fetch_ticker('BNB/USDT')
            self.assertIsNotNone(ticker)
            self.assertIn('last', ticker)
            print("CCXT Binance Data:", ticker)
        except Exception as e:
            self.fail(f"Fetching data from Binance via CCXT failed: {e}")

    # Test fetching data from CoinGecko API
    def test_fetch_coingecko(self):
        cg = CoinGeckoAPI()
        try:
            usdt_data = cg.get_coin_market_chart_by_id('tether', 'usd', 365)  
            self.assertIsNotNone(usdt_data)
            self.assertIn('prices', usdt_data)
            print("CoinGecko Data:", usdt_data)
        except Exception as e:
            self.fail(f"Fetching data from CoinGecko failed: {e}")

    # Test fetching Ethereum data using Web3
    def test_fetch_web3(self):
        infura_url = os.getenv('INFURA_HTTPS_URL')  
        w3 = Web3(Web3.HTTPProvider(infura_url))
        
        if w3.is_connected():
            eth_block = w3.eth.get_block('latest')
            self.assertIsNotNone(eth_block)
            self.assertIn('number', eth_block)
            print("Ethereum Data (Latest Block):", eth_block)
        else:
            self.fail("Failed to connect to Ethereum via Web3")


    # Test fetching data from Messari API
    def test_fetch_messari(self):
        url = 'https://data.messari.io/api/v1/assets/usdc/metrics'
        try:
            response = requests.get(url)
            data = response.json()
            self.assertIsNotNone(data)
            self.assertIn('data', data)
            print("Messari Data:", data)
        except Exception as e:
            self.fail(f"Fetching data from Messari failed: {e}")

    # Test fetching data from yFinance (Traditional Finance)
    def test_fetch_yfinance(self):
        try:
            yields = yf.download('^TNX', period='1y')
            self.assertIsNotNone(yields)
            self.assertGreater(len(yields), 0)
            print("yFinance Data (Treasury Yields):", yields)
        except Exception as e:
            self.fail(f"Fetching data from yFinance failed: {e}")

if __name__ == '__main__':
    unittest.main()

