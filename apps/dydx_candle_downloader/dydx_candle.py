from datetime import datetime, timedelta, timezone

from utils.dydx_client import DydxClient
from utils.logger import setup_logger


def main():
    logger = setup_logger(name="data_downloader_logger", folder_path=".//data")

    logger.info("Program start.")

    market = "BTC-USD"
    end = datetime.utcnow().replace(
        minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    start = end - timedelta(hours=110)

    dydx_client = DydxClient(logger=logger)

    data = dydx_client.get_market_candle(market, start, end)
    logger.info(data)

    logger.info("Program end.")


if __name__ == "__main__":
    main()
