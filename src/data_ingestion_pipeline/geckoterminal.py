import requests, csv
from datetime import datetime, timezone, timedelta
POOL_ID    = "C9U2Ksk6KKWvLEeo5yUQ7Xu46X7NzeBJtd9PBfuXaUSM"
NETWORK    = "solana"
TIMEFRAME  = "day"   # allowed: day, hour, minute
OUTPUT_CSV = "data/raw/Fartcoin_SOL_last2yrs.csv"
# Fetch the entire history
URL = (
    f"https://api.geckoterminal.com/api/v2/networks/{NETWORK}"
    f"/pools/{POOL_ID}/ohlcv/{TIMEFRAME}"
)
resp    = requests.get(URL)
resp.raise_for_status()
ohlcv   = resp.json()["data"]["attributes"]["ohlcv_list"]  
# Compute cutoff = now − 2 years (approx 730 days)
now        = datetime.now(timezone.utc)
cutoff_dt  = now - timedelta(days=365*2)
cutoff_ts  = int(cutoff_dt.timestamp())  # seconds since epoch
# Filter to only bars newer than cutoff
filtered = [ bar for bar in ohlcv if bar[0] >= cutoff_ts ]
# Write CSV
with open(OUTPUT_CSV, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["date","open","high","low","close","volume"])
    for ts, o, h, l, c, v in filtered:
        dt = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        writer.writerow([dt, o, h, l, c, v])
print(f":white_check_mark: Saved {OUTPUT_CSV} ({len(filtered)} daily bars, from {cutoff_dt.date()} to {now.date()})")