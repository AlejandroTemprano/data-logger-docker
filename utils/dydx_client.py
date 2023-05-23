import threading
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd
from dydx3 import Client
from dydx3.constants import API_HOST_MAINNET, NETWORK_ID_MAINNET

from utils.logger import setup_logger


@dataclass
class Resolution:
    DAY_1: str = "1DAY"
    HOURS_4: str = "4HOURS"
    HOURS_1: str = "1HOUR"
    MINS_30: str = "30MINS"
    MINS_15: str = "15MINS"
    MINS_5: str = "5MINS"
    MINS_1: str = "1MIN"


class DydxClient:
    """
    Handles all DYDX execution.
    """

    def __init__(self, logger=None):
        """ """
        self.lock = threading.RLock()

        self.logger = logger if logger else setup_logger(name="dydx_client_logger")

        self.public_client = Client(
            host=API_HOST_MAINNET,
            network_id=NETWORK_ID_MAINNET,
        )

    def get_market_candle(
        self, market: str, start: datetime, end: datetime
    ) -> pd.DataFrame:
        """Downloads candle data for market between start and end dates."""

        data = pd.DataFrame()
        end_date = end
        while end_date > start:
            try:
                raw_data = self.public_client.public.get_candles(
                    market,
                    resolution=Resolution.HOURS_1,
                    from_iso=datetime.strftime(
                        start - timedelta(hours=1), "%Y-%m-%d %H:%M:%S"
                    ),
                    to_iso=datetime.strftime(end_date, "%Y-%m-%d %H:%M:%S"),
                    limit="100",
                ).data["candles"]

            except Exception as e:
                self.logger.error(e)
                self.logger.error(f"Unable to get candle data for market {market}.")
                return pd.DataFrame()

            raw_data = pd.DataFrame.from_dict(raw_data)
            raw_data["startedAt"] = pd.to_datetime(raw_data["startedAt"], utc=True)

            data = pd.concat([data, raw_data], axis=0)
            end_date = raw_data.loc[raw_data["startedAt"].idxmin(), "startedAt"]

        data = data.drop_duplicates(subset="startedAt", keep="last")

        # Format data
        data = data.reset_index(drop=True)
        data["startedAt"] = pd.to_datetime(data["startedAt"], utc=True)
        data["updatedAt"] = pd.to_datetime(data["updatedAt"], utc=True)
        data["market"] = data["market"].astype(str)
        data["resolution"] = data["resolution"].astype(str)
        data["low"] = data["low"].astype(float)
        data["high"] = data["high"].astype(float)
        data["open"] = data["open"].astype(float)
        data["close"] = data["close"].astype(float)
        data["baseTokenVolume"] = data["baseTokenVolume"].astype(float)
        data["trades"] = data["trades"].astype(float)
        data["usdVolume"] = data["usdVolume"].astype(float)
        data["startingOpenInterest"] = data["startingOpenInterest"].astype(float)

        # Rename columns
        data = data.rename(
            columns={
                "startedAt": "date",
                "updatedAt": "updated",
                "low": "low_price",
                "high": "high_price",
                "open": "open_price",
                "close": "close_price",
                "baseTokenVolume": "volume",
                "baseTokenVolume": "volume_usd",
                "startingOpenInterest": "starting_interest",
            }
        )

        return data
