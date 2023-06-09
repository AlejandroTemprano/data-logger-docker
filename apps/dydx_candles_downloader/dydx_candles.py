"""
Script to download the last 2 dydx 1-hour candles every hour and store them into a postgresql db.

Db credentials are pulled from ./credentials/db_credentials.py.

Use setup.py before running this code to create the table inside the db and fill the table with historical data (if desired, check setup.py for more info).
"""

import signal
from datetime import datetime, timedelta, timezone
from time import sleep

from credentials.db_credentials import db_credentials as DB_CREDENTIALS
from utils.db_connector import DatabaseConnector
from utils.dydx_client import DydxClient
from utils.logger import setup_logger

TABLE_NAME = "dydx_candles"


def next_target(target_interval: int = 3600) -> datetime:
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    next_target = now + timedelta(seconds=target_interval - now.timestamp() % target_interval)
    return next_target


class DxdxCandleDownloader:
    def __init__(self, logger=None):
        self.logger = logger if logger else setup_logger(name=f"{TABLE_NAME}_downloader", folder_path=".//data")
        self.running: bool = False

        # Initialize the exchange and db clients
        self.exchange_client = DydxClient(self.logger)
        self.db_client = DatabaseConnector(DB_CREDENTIALS, self.logger)

    def run(self):
        self.logger.info("Program start.")
        self.running = True

        target = next_target()
        self.logger.info(f"Waiting for target: {target}")

        while self.running:
            time_now = datetime.utcnow().replace(tzinfo=timezone.utc)
            if time_now >= target:
                self.logger.info(f"Target reached.")

                # Gets start (current) time and start time.
                # It will download the las 2 candles to overwrite the previous one to ensure is up to date
                end = time_now.replace(minute=0, second=0, microsecond=0)
                start = end - timedelta(hours=2)

                # Downloading online markets
                markets_list = self.exchange_client.get_online_markets()

                # Download candles from the exchange
                download_start = datetime.now().replace(tzinfo=timezone.utc)
                candles = self.exchange_client.get_all_markets_candles(markets_list, start, end)
                download_end = datetime.now().replace(tzinfo=timezone.utc)

                # Saves candle data inside the db
                save_start = datetime.now().replace(tzinfo=timezone.utc)
                self.db_client.insert_candles(TABLE_NAME, candles)
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
