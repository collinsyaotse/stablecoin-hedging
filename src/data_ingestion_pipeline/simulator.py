import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta

# Load your actual CSV
fee_data = pd.read_csv("orca_fee_data.csv")
base_row = fee_data.iloc[0]  # Use first row for simulation

# Parameters
days = 730  # 2 years
np.random.seed(42)
end_date = pd.to_datetime(base_row['timestamp'])
dates = pd.date_range(end=end_date, periods=days)

# Simulation elements
base_volume = base_row['estimated_daily_volume']
seasonality = 0.1 * base_volume * np.sin(np.linspace(0, 10 * np.pi, days))
trend = np.linspace(0.95, 1.05, days)
noise = np.random.normal(0, 0.05 * base_volume, days)
simulated_volume = np.maximum(base_volume * trend + seasonality + noise, 0)

# Fee calculations
fee_rate = base_row['fee_rate_percent'] / 100
protocol_fee_rate = base_row['protocol_fee_rate_percent'] / 100
lp_share = base_row['lp_fee_share']

sim_lp_fees = simulated_volume * fee_rate * lp_share
sim_protocol_fees = simulated_volume * protocol_fee_rate

# Final DataFrame
sim_data = pd.DataFrame({
    'date': dates,
    'simulated_volume': simulated_volume,
    'simulated_lp_fees': sim_lp_fees,
    'simulated_protocol_fees': sim_protocol_fees
})

# Save to CSV
sim_data.to_csv("simulated_orca_fees_2_years.csv", index=False)

# Plot
plt.figure(figsize=(14, 6))
plt.plot(sim_data['date'], sim_data['simulated_lp_fees'], label='Simulated lp_fees')
plt.title("Simulated Orca Daily lp_fees Over 2 Years")
plt.xlabel("Date")
plt.ylabel("lp_fees")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
