from datetime import datetime, timedelta, timezone

from credentials.db_credentials import db_credentials as DB_CREDENTIALS
from utils.db_connector import DatabaseConnector
from utils.dydx_client import DydxClient
from utils.logger import setup_logger


def main():
    logger = setup_logger(name="data_downloader_logger", folder_path=".//data")

    logger.info("Program start.")

    dydx_client = DydxClient(logger)

    db_client = DatabaseConnector(DB_CREDENTIALS, logger)

    end = datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    start = end - timedelta(hours=1)
    candle = dydx_client.get_market_candle("BTC-USD", start, end)
    print(candle)
    logger.info("Program end.")


if __name__ == "__main__":
    main()
