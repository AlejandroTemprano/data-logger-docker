"""
Example on how to get data from the DB once its saved using dydx_candle.py and setup.py
"""
from datetime import datetime, timedelta, timezone

import pandas as pd
from db_connector import DatabaseConnector

# Import the credentials and the db client
from db_credentials import db_credentials as DB_CREDENTIALS

# Initialize the client
db_client = DatabaseConnector(DB_CREDENTIALS)

# Define the data to download
# This example will download the last day of candles for the markets inside market_list
end = datetime.utcnow().replace(tzinfo=timezone.utc)
start = end - timedelta(days=1)
market_list = ["ADA-USD", "BTC-USD", "ETH-USD"]

# Import the data from the DB
# If to_file = True, it will save the df to a pickle file inside file_path
candles = db_client.get_candles(market_list, start, end, to_file=True, file_path="./dydx_candles.pkl")

print(candles)
