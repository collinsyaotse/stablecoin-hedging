import os

import requests
from dotenv import load_dotenv
from pycoingecko import CoinGeckoAPI

import pandas as pd

load_dotenv()

def test_fetch_coingecko(self):
        """Test the CoinGecko API fetch function for expected data format."""
        cg = CoinGeckoAPI()
        try:
            usdt_data = cg.get_coin_market_chart_by_id('tether', 'usd', 365)
            data = usdt.json()
            self.assertIsNotNone(usdt_data)
            self.assertIn('prices', usdt_data)
            print("CoinGecko Data:", usdt_data)
        except Exception as err:
            self.fail(f"Fetching data from CoinGecko failed: {err}")


