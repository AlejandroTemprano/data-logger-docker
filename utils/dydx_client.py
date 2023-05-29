import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Union

import pandas as pd
from dydx3 import Client, DydxApiError
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


def max_requests(start_date: datetime, end_date: datetime, resolution: str) -> int:
    """Calculates the number of max requests needed to import all candles from the exchange."""
    resolution_deltas = {
        Resolution.DAY_1: timedelta(days=1),
        Resolution.HOURS_4: timedelta(hours=4),
        Resolution.HOURS_1: timedelta(hours=1),
        Resolution.MINS_30: timedelta(minutes=30),
        Resolution.MINS_15: timedelta(minutes=15),
        Resolution.MINS_5: timedelta(minutes=5),
        Resolution.MINS_1: timedelta(minutes=1),
    }

    delta = resolution_deltas[resolution]
    total_lines = ceil((end_date - start_date) / delta)

    return ceil(total_lines / 100)


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

        self.request_limit_getV3 = 175  # limit of request that can handle dydx in 10 seconds. Subject to change.

    def get_online_markets(self) -> list:
        """Downloads all markets and returns a list with only the ONLINE markets."""

        try:
            raw_data = self.public_client.public.get_markets().data["markets"]
        except Exception as e:
            self.logger.error(e)
            self.logger.error(f"Unable to get markets data.")
            return ()

        data = []
        for keys, values in raw_data.items():
            if values["status"] == "ONLINE":
                data.append(values["market"])

        return tuple(sorted(data))

    def get_market_candles(
        self,
        market: str,
        start: datetime,
        end: datetime,
        resolution: str = Resolution.HOURS_1,
    ) -> pd.DataFrame:
        """Downloads candle data for market between start and end dates."""

        data = pd.DataFrame()
        end_date = end

        while end_date > start:
            try:
                raw_data = self.public_client.public.get_candles(
                    market,
                    resolution=resolution,
                    from_iso=datetime.strftime(start - timedelta(hours=1), "%Y-%m-%d %H:%M:%S"),
                    to_iso=datetime.strftime(end_date, "%Y-%m-%d %H:%M:%S"),
                    limit="100",
                ).data["candles"]

            except DydxApiError as e:
                if e.status_code == 429:
                    self.logger.error("Exchange rate limit reached.")
                else:
                    self.logger.error(e)
                    self.logger.error(f"Unable to get candle data for market {market}.")
                raw_data = pd.DataFrame()
            except Exception as e:
                self.logger.error(e)
                self.logger.error(f"Unable to get candle data for market {market}.")
                raw_data = pd.DataFrame()

            raw_data = pd.DataFrame.from_dict(raw_data)

            if raw_data.empty:
                break

            raw_data["startedAt"] = pd.to_datetime(raw_data["startedAt"], utc=True)
            data = pd.concat([data, raw_data], axis=0)

            end_date = raw_data.loc[raw_data["startedAt"].idxmin(), "startedAt"]

        if data.empty:
            data = pd.DataFrame(
                columns=[
                    "date",
                    "updated",
                    "open_price",
                    "close_price",
                    "high_price",
                    "low_price",
                    "volume",
                    "volume_usd",
                ]
            )
            if threading.current_thread().name == f"get_market_candle({market})":
                with self.lock:
                    self.candles_data[market] = data
            else:
                return data

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
        data["trades"] = data["trades"].astype(int)
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
                "usdVolume": "volume_usd",
                "startingOpenInterest": "starting_interest",
            }
        )

        # Delete unused columns and rearrange the columns
        data = data[
            [
                "date",
                "updated",
                "market",
                "resolution",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "volume",
            ]
        ]

        # Check if its running from get_all_market_candles thread
        if threading.current_thread().name == f"get_market_candle({market})":
            with self.lock:
                self.candles_data[market] = data

        else:
            return data

    def get_all_markets_candles(
        self,
        market_list: Union[tuple, list],
        start: datetime,
        end: datetime,
        resolution: str = Resolution.HOURS_1,
    ) -> pd.DataFrame:
        """Downloads candle data for all markets inside market_list between start and end dates.
        Return a unique DataFrame with all data.

        Uses threading to send multiple request. When request limit is reached, it will sleep(10) and continue.
        """
        # Defines a variable to store all candle data from the threads
        self.candles_data = {
            market: pd.DataFrame(
                columns=[
                    "date",
                    "updated",
                    "open_price",
                    "close_price",
                    "high_price",
                    "low_price",
                    "volume",
                    "volume_usd",
                ]
            )
            for market in market_list
        }

        # Checks the max number of request needed to get the data
        requests = max_requests(start, end, resolution)

        # It will multithread only for 1 request to avoid reaching exchange limits
        if requests == 1:
            # Defines a dict of threads, starts them and joins them
            thread_dict = {}
            for market in market_list:
                thread_dict[market] = threading.Thread(
                    target=self.get_market_candles,
                    args=(market, start, end, resolution),
                    name=f"get_market_candle({market})",
                )

            for t in thread_dict.values():
                t.start()

            for t in thread_dict.values():
                t.join()
        # If there is too many requests, send one by one.
        else:
            for i, market in enumerate(market_list):
                self.logger.debug(f"Downloading {market} data... ({i+1}/{len(market_list)})")
                self.candles_data[market] = self.get_market_candles(market, start, end, resolution)

        # Merges all data candle df from the threads
        self.candles_data = pd.concat(self.candles_data.values(), ignore_index=True)
        self.candles_data = self.candles_data.sort_values(by="date", ascending=True)
        self.candles_data.reset_index(drop=True)

        return self.candles_data

    def get_market_list_6mo(self, number_of_markets: int = 25) -> tuple:
        """Get a market list with the first number_of_markets ONLINE markets by traded volume in usd."""
        end = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        start = end - timedelta(days=30 * 6)

        online_markets = self.get_online_markets()

        vol_traded_usd = {}
        for market in online_markets:
            candle_data = self.get_market_candle(market, start, end, resolution=Resolution.DAY_1)
            vol_usd = candle_data["volume"] * candle_data["close_price"]
            vol_traded_usd[market] = vol_usd.sum()

        vol_traded_usd = [
            market
            for market, vol in sorted(vol_traded_usd.items(), key=lambda x: x[1], reverse=True)[:number_of_markets]
        ]

        return tuple(sorted(vol_traded_usd))
