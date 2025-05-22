import pandas as pd
import os

# Define file paths
input_files = [
    "data/raw/JLP_SOL_90days.csv",
    "data/raw/JLP_USDC_90days.csv",
    "data/raw/Fartcoin_SOL_90days.csv"
]

# Load and tag each dataset
dfs = []
for file in input_files:
    df = pd.read_csv(file)
    df["source"] = os.path.basename(file).replace("_90days.csv", "")  # Add source column
    dfs.append(df)

# Merge all into a single DataFrame
merged_df = pd.concat(dfs, ignore_index=True)

# Save merged file
merged_df.to_csv("data/processed/geckoterminal/MERGED_POOLS_90days.csv", index=False)

print("✅ Merged CSV saved to data/processed/geckoterminal/MERGED_POOLS_90days.csv")
