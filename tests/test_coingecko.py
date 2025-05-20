import pandas as pd
from pycoingecko import CoinGeckoAPI

cg = CoinGeckoAPI()
print(cg.get_supported_vs_currencies())