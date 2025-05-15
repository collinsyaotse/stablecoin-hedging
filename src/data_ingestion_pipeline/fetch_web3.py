"""Module for fetching DeFi and Web3 data using APIs and Web3.py."""

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
import pandas as pd
import requests
from web3 import Web3

load_dotenv()

INFURA_URL = os.getenv("INFURA_HTTPS_URL")


def connect_to_web3():
    """Connects to Ethereum node using Web3 and returns the instance."""
    w3 = Web3(Web3.HTTPProvider(INFURA_URL))
    if not w3.is_connected():
        raise RuntimeError("Failed to connect to Ethereum via Web3.")
    return w3


def fetch_latest_block_info():
    """Fetches the latest Ethereum block information."""
    try:
        w3 = connect_to_web3()
        block = w3.eth.get_block("latest")
        if block and "number" in block:
            print("Latest Ethereum Block Info:", block)
            return block
        raise RuntimeError("Block data is incomplete.")
    except Exception as err:
        raise RuntimeError(f"Fetching block data failed: {err}") from err


def get_dai_collateralization_history(days_back=730):
    """
    Fetches DAI collateralization history (TVL) from MakerDAO protocol.

    Args:
        days_back (int): Number of days back to include.

    Returns:
        pd.DataFrame: Time series data of DAI collateralization.
    """
    url = "https://api.llama.fi/protocol/makerdao"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        history = data.get("tvl", [])
        df = pd.DataFrame(history)
        df["date"] = pd.to_datetime(df["date"], unit="s")
        df = df[df["date"] > (datetime.today() - timedelta(days=days_back))]
        csv_path = "data/raw/web3/Collateralized_history_DAI.csv"
        df.to_csv(csv_path, index=False)
        print(f"DAI collateralization history saved to {csv_path}")
        return df
    except Exception as err:
        raise RuntimeError(f"Error fetching DAI collateralization history: {err}") from err


def get_liquidity_pool_depths(pool_platform="curve"):
    """
    Fetches TVL (liquidity pool depths) from DeFiLlama for a pool platform.

    Args:
        pool_platform (str): Platform name for file naming (e.g., "curve").

    Returns:
        pd.DataFrame: Sorted TVL data by USD value.
    """
    url = "https://yields.llama.fi/chart/747c1d2a-c668-4682-b9f9-296708a3dd90"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data["data"])
        df["tvlUsd"] = df["tvlUsd"].astype(float)
        df.sort_values(by="tvlUsd", ascending=False, inplace=True)
        csv_path = f"data/raw/web3/{pool_platform}_liquidity_pool_depths.csv"
        df.to_csv(csv_path, index=False)
        print(f"Liquidity pool depths saved to {csv_path}")
        return df
    except Exception as err:
        raise RuntimeError(f"Error fetching liquidity pool depths: {err}") from err


if __name__ == "__main__":
    # You can call your desired functions here for quick testing
    fetch_latest_block_info()
    get_dai_collateralization_history()
    get_liquidity_pool_depths("curve")
