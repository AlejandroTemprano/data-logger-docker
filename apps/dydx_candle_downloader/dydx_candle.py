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


class DxdxCandleDownloader:
    def __init__(self, logger=None):
        self.logger = logger if logger else setup_logger(name="dydx_candle_downloader", folder_path=".//data")
        self.running: bool = False

        # Initialize the exchange and db clients
        self.dydx_client = DydxClient(logger)
        self.db_client = DatabaseConnector(DB_CREDENTIALS, logger)

    def run(self):
        self.logger.info("Program start.")
        self.running = True

        target = next_target()
        self.logger.info(f"Waiting for target: {target}")

        while self.running:
            time_now = datetime.utcnow().replace(tzinfo=timezone.utc)
            if time_now >= target:
                self.logger.info(f"Target reached.")

                end = time_now.replace(minute=0, second=0, microsecond=0)
                start = end - timedelta(hours=2)

                # Download candles from the exchange
                download_start = datetime.now().replace(tzinfo=timezone.utc)
                candles = self.dydx_client.get_all_markets_candles(MARKET_LIST, start, end)
                download_end = datetime.now().replace(tzinfo=timezone.utc)

                # Saves candle data inside the db
                save_start = datetime.now().replace(tzinfo=timezone.utc)
                self.db_client.insert_candles("dydx_candles", candles)
                save_end = datetime.now().replace(tzinfo=timezone.utc)

                target = next_target()

                self.logger.debug(f"Download time: {download_end-download_start}. Save time: {save_end-save_start}.")

                self.logger.info(f"Waiting for target: {target}")

            sleep(0.1)

        self.logger.info("Program end.")


def main():
    def signal_handler(signum, frame):
        client.running = False
        client.logger.info("Ending program. Waiting for cycle to finish...")

    signal.signal(signal.SIGINT, signal_handler)

    client = DxdxCandleDownloader()
    client.run()


if __name__ == "__main__":
    main()
