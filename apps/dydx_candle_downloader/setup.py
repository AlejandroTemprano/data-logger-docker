"""
setup.py for dydx_candle.py

Run this program before dydx_candle.py to set up the tables in the database.

This program will create a settings table if not already exist, save the basic settings including the market list and create a candle table.

If IMPORT_HISTORICAL_CANDLES == True, this program will download and save historical data.
"""

from datetime import datetime, timezone

import pandas as pd

from credentials.db_credentials import db_credentials as DB_CREDENTIALS
from data.config import DYDX_MARKET_LIST_ALL as MARKET_LIST
from utils.db_connector import DatabaseConnector
from utils.dydx_client import DydxClient
from utils.logger import setup_logger

IMPORT_HISTORICAL_CANDLES = True
START_DATE = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
END_DATE = None  # If none, it will use current date


def setup(logger=None):
    logger = logger if logger else setup_logger(name="db_client_logger", debug_level="DEBUG")
    logger.info("Starting setup for dydx_candle.py")

    db_client = DatabaseConnector(DB_CREDENTIALS, logger)

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
            volume REAL
        );
    """
    msg = db_client.send_request(sql)
    if msg != "CREATE TABLE":
        logger.error(msg)
        logger.error("Something happened creating the table, exiting.")
        return
    logger.info("Candle table created/already exist.")

    # Set up indexes for candle table
    sql = """
        CREATE UNIQUE INDEX IF NOT EXISTS candle_idx_date_market 
            ON dydx_candles (date, market);
    """
    msg = db_client.send_request(sql)
    if msg != "CREATE INDEX":
        logger.error(msg)
        logger.error("Something happened creating the index, exiting.")
        return
    logger.info("Candle index created/already exist.")

    # Import historical candle data from exchange and fill the table
    if IMPORT_HISTORICAL_CANDLES:
        debug_time = datetime.now()
        # Calculate end_date if None
        end_date = (
            datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            if END_DATE is None
            else END_DATE
        )

        logger.info(f"Downloading candle date between {START_DATE} and {end_date}...")

        dydx_client = DydxClient(logger)

        # Downloads the historical candles for each market
        download_time = datetime.now()
        candle_data = dydx_client.get_all_markets_candles(MARKET_LIST, START_DATE, end_date)
        logger.debug(f"Data downloaded in {datetime.now() - download_time}")
        logger.info("All candle data downloaded.")

        logger.info(f"Saving candle date into dydx_candle table...")
        insert_time = datetime.now()
        db_client.insert_candles(table_name="dydx_candles", candle_data=candle_data)

        logger.debug(f"Save the downloaded data to the db took {datetime.now() - insert_time}.")
        logger.debug(f"Total time to download and save candle data to the db: {datetime.now() - debug_time}.")

    logger.info("Setup complete.")
    return


def main():
    setup()


if __name__ == "__main__":
    main()
