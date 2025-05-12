import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def merge_and_clean_funding_rates(input_dir, output_file):
    # Dictionary to hold dataframes
    df_dict_funding = {}

    # Read all CSV files in the input directory
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(input_dir, file_name)
            df = pd.read_csv(file_path)

            # Convert timestamp to datetime and sort
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')

            # Drop duplicate timestamps (if any)
            df = df.drop_duplicates(subset=['timestamp', 'pair'])

            # Optionally handle zeros (e.g., replace with NaN then forward-fill)
            df['rate'] = df['rate'].replace(0, pd.NA)
            df['rate'] = df['rate'].ffill().infer_objects(copy=False)

            # Extract pair info
            df['pair_clean'] = df['pair'].str.replace('/', '_').str.replace(':', '_').str.lower()
            df[['base', 'quote']] = df['pair'].str.split('/', expand=True)
            df['quote'] = df['quote'].str.replace(':USDC', '', regex=False)  # optional cleanup

            # Reset index for safety
            df.reset_index(drop=True, inplace=True)

            # Normalize the rate column
            scaler = MinMaxScaler()
            df['rate_norm'] = scaler.fit_transform(df[['rate']])

            # Add to dictionary
            df_dict_funding[file_name] = df

    # Merge all dataframes vertically
    merged_funding_df = pd.concat(df_dict_funding.values(), axis=0)

    # Optional: Reset index if you don't want timestamps as the index
    merged_funding_df.reset_index(drop=True, inplace=True)

    # Save merged version
    merged_funding_df.to_csv(output_file, index=False)

# Define input directory and output file
input_directory = 'data/raw/binance/funding-rates'
output_csv = 'data/processed/binance/merged_funding_data.csv'

# Run the function
merge_and_clean_funding_rates(input_directory, output_csv)