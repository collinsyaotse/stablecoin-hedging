import requests, csv
import pandas as pd
from datetime import datetime

POOL_ID    = "D1qM4rMDmSzjarCTXrr1dynmDCP6DPQNkMe5m7UYnZh3"
NETWORK    = "solana"
TIMEFRAME  = "day"
OUTPUT_CSV = "data/raw/imputed_JLP_SOL_all_history_interpolated.csv"

# 1) Fetch the full history
URL = (
    f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
    f"/pools/{POOL_ID}/ohlcv/{TIMEFRAME}"
)
resp  = requests.get(URL)
resp.raise_for_status()
ohlcv = resp.json()["data"]["attributes"]["ohlcv_list"]

# 2) Convert to DataFrame
df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
df["date"] = pd.to_datetime(df["timestamp"], unit="s")
df.set_index("date", inplace=True)
df = df.drop(columns="timestamp")
df = df.sort_index()

# 3) Create complete date range from 2023-05-20 to most recent
full_range = pd.date_range(start="2023-05-20", end=df.index.max(), freq="D")
df = df.reindex(full_range)

# 4) Interpolate missing values using time-based interpolation
df = df.interpolate(method="time", limit_direction="both")

# 5) Write to CSV
df.reset_index(inplace=True)
df.rename(columns={"index": "date"}, inplace=True)
df.to_csv(OUTPUT_CSV, index=False)

<<<<<<< HEAD
print(f"✅ Saved interpolated CSV from 2023-05-20 to {df['date'].max().date()} ({len(df)} rows)")
=======
print(f"✅ Saved interpolated CSV from 2023-05-20 to {df['date'].max().date()} ({len(df)} rows)")
>>>>>>> main
