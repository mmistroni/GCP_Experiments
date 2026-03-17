from fredapi import Fred
import os
# https://medium.com/@wl8380/how-the-money-supply-secretly-drives-markets-and-how-to-track-it-with-python-8235a0e57eed
# Initialize with your API key
# Tip: Use os.environ.get('FRED_API_KEY') to keep it secure
fred = Fred(api_key=os.environ['FRED_KEY'])

# Get the M2 Money Stock (Seasonally Adjusted)
# M2SL is the standard monthly series
m2_data = fred.get_series('M2SL')

# To get the most recent value
latest_m2 = m2_data.iloc[-1]
prev_m2 = m2_data.iloc[-2]
latest_date = m2_data.index[-1]
prev_date = m2_data.index[-2]

print(f"Latest M2 Money Stock ({latest_date.date()}): {latest_m2} Billion")
print(f"Prev M2 Money Stock ({prev_date.date()}): {prev_m2} Billion")