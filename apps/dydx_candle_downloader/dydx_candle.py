"""
Script to download dydx candles and store them into a postgresql db.

It will download the previous two 1-hours candles each hour (since the previous candle si not up to date after a while) and save them.

Db credentials are pulled from ./utils/db_credentials.py.

Use setup.py before running this code to create the table inside the db and fill the table with historical data (if desired, check setup.py for more info).
"""

import signal
from datetime import datetime, timedelta, timezone
from time import sleep

from credentials.db_credentials import db_credentials as DB_CREDENTIALS
from data.config import DYDX_MARKET_LIST_ALL as MARKET_LIST
from utils.db_connector import DatabaseConnector
from utils.dydx_client import DydxClient
from utils.logger import setup_logger


def next_target(target_interval: int = 3600) -> datetime:
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    next_target = now + timedelta(seconds=target_interval - now.timestamp() % target_interval)
    return next_target


def main():
    running = True

    """
    def signal_handler(signum, frame):
        running = False
        logger.info("Ending program. Waiting for cycle to finish...")

    signal.signal(signal.SIGINT, signal_handler)
    """

    logger = setup_logger(name="dydx_candle_logger", folder_path=".//data")
    logger.info("Program start.")

    # Initialize the exchange and db clients
    dydx_client = DydxClient(logger)
    db_client = DatabaseConnector(DB_CREDENTIALS, logger)

    end = datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    start = end - timedelta(hours=2)

    target = next_target()
    logger.info(f"Waiting for target: {target}")
    while running:
        if datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc) >= target:
            logger.info(f"Target reached.")
            # Download candles from the exchange
            download_start = datetime.now().replace(tzinfo=timezone.utc)
            candles = dydx_client.get_all_markets_candles(MARKET_LIST, start, end)
            download_end = datetime.now().replace(tzinfo=timezone.utc)

            # Saves candle data inside the db
            save_start = datetime.now().replace(tzinfo=timezone.utc)
            db_client.insert_candles("dydx_candles", candles)
            save_end = datetime.now().replace(tzinfo=timezone.utc)

            target = next_target()

            logger.debug(f"Download time: {download_end-download_start}. Save time: {save_end-save_start}.")

            logger.info(f"Waiting for target: {target}")

        sleep(0.1)

    logger.info("Program end.")


if __name__ == "__main__":
    main()
