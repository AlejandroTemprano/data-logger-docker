"""
setup.py for dydx_candle.py

Run this program before dydx_candle.py to set up the tables in the database.

This program will create a settings table if not already exist, save the basic settings including the market list and create a candle table.

If IMPORT_HISTORICAL_CANDLES == True, this program will download and save historical data.

"""

from datetime import datetime, timezone

import pandas as pd

from credentials.db_credentials import db_credentials as DB_CREDENTIALS
from data.config import DYDX_MARKET_LIST_VOL25 as MARKET_LIST
from utils.db_connector import DatabaseConnector
from utils.dydx_client import DydxClient
from utils.logger import setup_logger

IMPORT_HISTORICAL_CANDLES = True
START_DATE = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
START_DATE = datetime(2023, 5, 24, 0, 0, 0, tzinfo=timezone.utc)


def setup(logger=None):
    logger = logger if logger else setup_logger(name="db_client_logger")

    db_client = DatabaseConnector(DB_CREDENTIALS)

    # Create candles table
    sql = """
        CREATE TABLE IF NOT EXISTS dydx_candles (
            id BIGSERIAL PRIMARY KEY,
            date TIMESTAMP WITH TIME ZONE,
            updated TIMESTAMP WITH TIME ZONE,
            market TEXT,
            resolution TEXT,
            open_price REAL,
            close_price REAL,
            high_price REAL,
            low_price REAL,
            volume REAL,
            volume_usd REAL,
            trades INTEGER,
            starting_interest REAL
        );
    """
    msg = db_client.send_request(sql)
    if msg != "CREATE TABLE":
        print(msg)
        print("Something happened creating the table, exiting.")
        return
    print("Candle table created/already exist.")

    # Set up indexes for candle table
    sql = """
        CREATE UNIQUE INDEX IF NOT EXISTS candle_idx_date_market 
            ON dydx_candles (date, market);
    """
    msg = db_client.send_request(sql)
    if msg != "CREATE INDEX":
        print(msg)
        print("Something happened creating the index, exiting.")
        return
    print("Candle index created/already exist.")

    # Set up table permissions
    sql = """
        GRANT SELECT ON TABLE dydx_candles TO "read_only";
        GRANT ALL ON TABLE dydx_candles TO "developer";
        GRANT ALL ON TABLE dydx_candles TO "logger";
    """
    msg = db_client.send_request(sql)
    if msg != "GRANT":
        print(msg)
        print("Something happened granting permissions, exiting.")
        return
    print("Permissions granted.")

    # Import historical candle data from exchange and fill the table
    if IMPORT_HISTORICAL_CANDLES:
        MARKET_LIST = ("ADA-USD", "ATOM-USD", "AVAX-USD")
        end_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        print(f"Downloading candle date between {START_DATE} and {end_date}...")

        dydx_client = DydxClient(logger)

        candle_data = {}
        for i, market in enumerate(MARKET_LIST):
            print(f"Downloading {market}, candle {i+1}/{len(MARKET_LIST)} data... ")
            candle_data[market] = dydx_client.get_market_candle(market, START_DATE, end_date)
        print("All candle data downloaded.")

        candle_data = pd.concat(candle_data.values(), ignore_index=True)
        print(candle_data)

    return


def main():
    setup()


if __name__ == "__main__":
    main()
