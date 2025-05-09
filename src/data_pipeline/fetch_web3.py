import os
from dotenv import load_dotenv
import pandas as pd
import requests
from web3 import Web3
from datetime import datetime, timedelta

load_dotenv()

def fetch_historical_data(w3, contract_address, start_block, end_block):
    infura_url = os.getenv("INFURA_HTTPS_URL")
    w3 = Web3(Web3.HTTPProvider(infura_url))
    if w3.is_connected():
        try:
            eth_block = w3.eth.get_block('latest')
            assert eth_block is not None
            assert 'number' in eth_block
            print("Ethereum Data (Latest Block):", eth_block)
        except Exception as err:
            raise RuntimeError(f"Fetching block data via Web3 failed: {err}")
    else:
        raise RuntimeError("Failed to connect to Ethereum via Web3")


### 1. Get Collateralization Ratio for DAI via MakerDAO (DeFiLlama API)
def get_dai_collateralization_history():
    url = "https://api.llama.fi/protocol/makerdao"
    response = requests.get(url)
    data = response.json()
    csv_filename = f"data/raw/web3/Collateralized_history.csv"
    # Extract TVL and backing over time
    history = data.get("tvl", [])
    df = pd.DataFrame(history)
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df = df[df['date'] > (datetime.today() - timedelta(days=730))]  # Last 2 years
    df.to_csv(csv_filename, index=False)
    print(f"Collateralized_history saved to {csv_filename}")
    return df


### 2. Get Treasury Composition of a Protocol (e.g., Lido) from DeFiLlama
def get_treasury_composition(protocol="lido"):
    url = f"https://api.llama.fi/treasuries/{protocol}"
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.json_normalize(response.json()["tokens"])
        csv_filename = f"data/raw/web3/{protocol}_treasury_composition.csv"
        df.to_csv(csv_filename, index=False)
        print(f"Treasury composition saved to {csv_filename}")
        return df
    if response.status_code == 200:
        return pd.json_normalize(response.json()["tokens"])
    else:
        raise Exception(f"Treasury data fetch failed for {protocol}")


### 3. Get Liquidity Pool Depths (TVL) from DeFiLlama Aggregated Pools
def get_liquidity_pool_depths(pool_platform="curve"):
    url = f"https://api.llama.fi/pools"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Failed to fetch pool data.")
    data = response.json()

    df = pd.DataFrame(data['data'])
    df = df[df['project'] == pool_platform]
    df['tvlUsd'] = df['tvlUsd'].astype(float)
    csv_filename = f"data/raw/web3/{pool_platform}_liquidity_pool_depths.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Liquidity pool depths saved to {csv_filename}")
    df = df[['symbol', 'tvlUsd', 'project', 'chain']]
    return df.sort_values(by='tvlUsd', ascending=False)

### 4. Get Historical Data for a Specific Contract
def get_historical_data(contract_address, start_block, end_block):
    url = f"https://api.llama.fi/protocol/makerdao"
    response = requests.get(url)
    data = response.json()
    if response.status_code == 200:
        df = pd.json_normalize(data["tokens"])
        csv_filename = f"data/raw/web3/{contract_address}_historical_data.csv"
        df.to_csv(csv_filename, index=False)
        print(f"Historical data saved to {csv_filename}")
        return df
    else:
        raise Exception("Failed to fetch historical data.")
    
if __name__ == "__main__":
    # Example usage
    w3 = Web3(Web3.HTTPProvider(os.getenv("INFURA_HTTPS_URL")))
    contract_address = "0x6B175474E89094C44Da98b954EedeAC495271d0F"  # DAI contract address
    start_block = 0
    end_block = 99999999

    get_dai_collateralization_history()
    get_treasury_composition()
    get_liquidity_pool_depths()
    get_historical_data(contract_address, start_block, end_block)
    # Fetch historical data for DAI
    fetch_historical_data(w3, contract_address, start_block, end_block)
    # Fetch treasury composition for Lido
    get_treasury_composition("lido")
    # Fetch liquidity pool depths for Curve
    get_liquidity_pool_depths("curve")
    

