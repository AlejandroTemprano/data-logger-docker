"""
setup.py for dydx_candle.py

Run this program before dydx_candle.py to set up the table in the database.

If IMPORT_HISTORICAL_CANDLES == True, this program will download and save historical data.
If DELETE_PREVIOUS_DATA = True it will delete all previous data before importing the new data. If false no data will be imported.
"""

from datetime import datetime, timezone

import pandas as pd

from credentials.db_credentials import db_credentials as DB_CREDENTIALS
from utils.db_connector import DatabaseConnector
from utils.dydx_client import DydxClient
from utils.logger import setup_logger

IMPORT_HISTORICAL_CANDLES = True
DELETE_PREVIOUS_DATA = True

EXCHANGE_START_DATE = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
TABLE_NAME = "dydx_candles"


def setup(logger=None):
    logger = logger if logger else setup_logger(name="db_client_logger", debug_level="DEBUG")
    logger.info("Starting setup for dydx_candle.py")

    # Initialize clients
    db_client = DatabaseConnector(DB_CREDENTIALS, logger)
    exchange_client = DydxClient(logger)

    # Create candles table
    sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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

    # Set up indexes for candles table
    sql = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {TABLE_NAME}_idx_date_market 
            ON {TABLE_NAME} (date, market);
    """
    msg = db_client.send_request(sql)
    if msg != "CREATE INDEX":
        logger.error(msg)
        logger.error("Something happened creating the index, exiting.")
        return
    logger.info("Candle index created/already exist.")

    # Checks for previous data inside the table
    sql = f"""
        SELECT market, MIN(date) as first_candle, MAX(date) as last_candle
        FROM {TABLE_NAME}
        GROUP BY market;
    """
    data = db_client.send_query(sql)
    data = {
        market: {"first_candle": first_candle, "last_candle": last_candle} for market, first_candle, last_candle in data
    }

    if IMPORT_HISTORICAL_CANDLES:
        # With previous data present:
        if bool(data):
            # DO nothing
            if not DELETE_PREVIOUS_DATA:
                logger.info("Previous data is present. Keeping current data.")
                logger.info("Setup complete.")
                return

            # Erase all data
            else:
                logger.info("Previous data is present. Erasing previous data.")
                sql = f"""TRUNCATE TABLE {TABLE_NAME} RESTART IDENTITY;"""
                msg = db_client.send_request(sql)
                if msg != "TRUNCATE TABLE":
                    logger.error(msg)
                    logger.error("Something happened erasing the table, exiting.")
                    return
                logger.info(f"Table {TABLE_NAME} truncated.")
        else:
            logger.info("No previous data found")

        logger.info("Downloading all available candle data from the exchange.")

        # Download list with all online markets
        markets_list = exchange_client.get_online_markets()

        # Import historical candle data from exchange and fill the table
        debug_time = datetime.now()

        end_date = datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        logger.info(f"Downloading candle date between {EXCHANGE_START_DATE} and {end_date}...")

        # Downloads the historical candles for each market
        download_time = datetime.now()
        candles_data = exchange_client.get_all_markets_candles(markets_list, EXCHANGE_START_DATE, end_date)
        logger.debug(f"Data downloaded in {datetime.now() - download_time}")
        logger.info("All candle data downloaded.")

        logger.info(f"Saving candle date into dydx_candle table...")
        insert_time = datetime.now()
        db_client.insert_candles(table_name="dydx_candles", candles_data=candles_data)

        logger.debug(f"Save the downloaded data to the db took {datetime.now() - insert_time}.")
        logger.debug(f"Total time to download and save candle data to the db: {datetime.now() - debug_time}.")

    logger.info("Setup complete.")
    return


def main():
    setup()


if __name__ == "__main__":
    main()
