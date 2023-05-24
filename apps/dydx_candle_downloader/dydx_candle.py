from datetime import datetime, timedelta, timezone

from utils.dydx_client import DydxClient, Resolution
from utils.logger import setup_logger


def main():
    logger = setup_logger(name="data_downloader_logger", folder_path=".//data")

    logger.info("Program start.")

    dydx_client = DydxClient(logger=logger)

    logger.info("Program end.")


if __name__ == "__main__":
    main()
