import asyncio
import aiohttp
import os
import csv
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional
from dotenv import load_dotenv
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from orca_whirlpool.constants import ORCA_WHIRLPOOL_PROGRAM_ID
from orca_whirlpool.context import WhirlpoolContext
from orca_whirlpool.utils import PriceMath, DecimalUtil

load_dotenv()

CSV_FILE = "orca_fee_data.csv"

class OrcaFeeTracker:
    def __init__(self, rpc_endpoint: str = "https://api.mainnet-beta.solana.com"):
        self.rpc_endpoint = rpc_endpoint
        self.whirlpool_program_id = ORCA_WHIRLPOOL_PROGRAM_ID
        self.sol_pools = {
            "JLP/SOL": "D1qM4rMDmSzjarCTXrr1dynmDCP6DPQNkMe5m7UYnZh3",
            "JLP/USDC": "6NUiVmsNjsi4AfsMsEiaezsaV9N4N1ZrD4jEnuWNRvyb",
        }

    async def get_whirlpool_data(self, pool_name: str) -> Optional[Dict]:
        try:
            connection = AsyncClient(self.rpc_endpoint)
            ctx = WhirlpoolContext(self.whirlpool_program_id, connection, Keypair())
            whirlpool_pubkey = Pubkey.from_string(self.sol_pools[pool_name])
            whirlpool = await ctx.fetcher.get_whirlpool(whirlpool_pubkey)
            decimals_a = (await ctx.fetcher.get_token_mint(whirlpool.token_mint_a)).decimals
            decimals_b = (await ctx.fetcher.get_token_mint(whirlpool.token_mint_b)).decimals
            price = PriceMath.sqrt_price_x64_to_price(
                whirlpool.sqrt_price, decimals_a, decimals_b
            )
            await connection.close()
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "pool_name": pool_name,
                "pool_address": str(whirlpool_pubkey),
                "price": float(DecimalUtil.to_fixed(price, decimals_b)),
                "liquidity": whirlpool.liquidity,
                "fee_rate": whirlpool.fee_rate,
                "protocol_fee_rate": whirlpool.protocol_fee_rate,
            }
        except Exception as e:
            print(f"[ERROR] Fetching whirlpool data for {pool_name}: {e}")
            return None

    async def calculate_fee_estimates(self, pool_data: Dict) -> Optional[Dict]:
        try:
            fee_rate_percent = pool_data["fee_rate"] / 10000
            protocol_fee_rate_percent = pool_data["protocol_fee_rate"] / 10000
            lp_fee_share = 0.87
            estimated_daily_volume = pool_data["liquidity"] * 0.1
            total_daily_fees = estimated_daily_volume * fee_rate_percent
            lp_daily_fees = total_daily_fees * lp_fee_share
            protocol_daily_fees = total_daily_fees * (1 - lp_fee_share)
            return {
                **pool_data,
                "fee_rate_percent": fee_rate_percent,
                "protocol_fee_rate_percent": protocol_fee_rate_percent,
                "estimated_daily_volume": estimated_daily_volume,
                "estimated_lp_daily_fees": lp_daily_fees,
                "estimated_protocol_daily_fees": protocol_daily_fees,
                "lp_fee_share": lp_fee_share
            }
        except Exception as e:
            print(f"[ERROR] Calculating fee estimates: {e}")
            return None

    def append_to_csv(self, data: Dict):
        fieldnames = list(data.keys())
        file_exists = os.path.isfile(CSV_FILE)
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)


async def main():
    tracker = OrcaFeeTracker()
    for pool_name in tracker.sol_pools.keys():
        print(f"[INFO] Checking {pool_name}...")
        pool_data = await tracker.get_whirlpool_data(pool_name)
        if pool_data:
            estimates = await tracker.calculate_fee_estimates(pool_data)
            if estimates:
                tracker.append_to_csv(estimates)
                print(f"[SAVED] Data saved for {pool_name} at {estimates['timestamp']}")
        else:
            print(f"[WARN] Could not fetch data for {pool_name}.")

if __name__ == "__main__":
    asyncio.run(main())
