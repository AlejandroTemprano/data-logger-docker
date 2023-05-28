"""
Run this file to import dydx candles from the DB. Just change the settings.

pysicopg3 should be installed:

pip install psycopg[binary]

All dydx markets should be available. 1-hour candles only.

A dataframe with all the data will be saved on ./dydx_candles.pkl
"""
from datetime import datetime, timedelta, timezone
from typing import Union

import pandas as pd
import psycopg

from utils.db_connector import DatabaseConnector

### SETTINGS ###
end = datetime.utcnow().replace(tzinfo=timezone.utc)
start = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
start = end - timedelta(days=120)

db_credentials = {
    "host": "localhost",
    "port": "5432",
    "database": "dydx_data",
    "user": "user_example",
    "password": "password",
}

market_list = [
    "ADA-USD",
    "ETH-USD",
]


class DatabaseConnector:
    """
    Class to interact with the database.
    """

    def __init__(self, db_credentials: dict):
        self.conninfo_str = f"""
        host={db_credentials["host"]}
        port={db_credentials["port"]}
        dbname={db_credentials["database"]}
        user={db_credentials["user"]}
        password={db_credentials["password"]}
        """

    def send_query(self, sql: str, values: tuple = ()) -> str:
        """Sends sql query to the db and return all data queried."""
        try:
            with psycopg.connect(self.conninfo_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, values)
                    data = cur.fetchall()
                    conn.commit()

        except psycopg.Error as e:
            print(e)
            print("Unable to execute command.")
            return ""

        return data

    def get_candles(
        self,
        market_list: Union[tuple, list, str],
        start: datetime = None,
        end: datetime = datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc),
        table_name: str = "dydx_candles",
        to_file: bool = False,
        file_path: str = "./dydx_candles.pkl",
    ):
        start = end - timedelta(hours=1) if start == None else start
        market_list = [market_list] if isinstance(market_list, str) else market_list

        market_placeholders = ",".join(["%s"] * len(market_list))

        sql = f"""
            SELECT date, market, resolution, open_price, close_price, high_price, low_price, volume FROM {table_name}
            WHERE date BETWEEN %s AND %s
            AND market IN ({market_placeholders})
        """

        # Add the start and end values to the values tuple
        values = (start, end) + tuple(market_list)

        # Send the query
        data = self.send_query(sql, values)

        candle_data = pd.DataFrame(
            data,
            columns=["date", "market", "resolution", "open_price", "close_price", "high_price", "low_price", "volume"],
        )

        candle_data = candle_data.astype(
            {
                "date": str,
                "market": str,
                "resolution": str,
                "open_price": float,
                "close_price": float,
                "high_price": float,
                "low_price": float,
                "volume": float,
            }
        )

        candle_data["date"] = pd.to_datetime(candle_data["date"], utc=True)
        candle_data = candle_data.sort_values(by="date").reset_index(drop=True)

        if to_file:
            candle_data.to_pickle(file_path)

        return candle_data


def main():
    print(f"Downloading dydx candles from {start} to {end}.")
    print(f"Selected markets: {market_list}.")

    time_start = datetime.now()
    db_client = DatabaseConnector(db_credentials)

    candles = db_client.get_candles(market_list, start, end, to_file=True, file_path="./dydx_candles.pkl")

    print(candles)
    print(f"Done in {(datetime.now() - time_start).seconds}s.")


if __name__ == "__main__":
    main()
