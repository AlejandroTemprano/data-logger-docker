import os

import pandas as pd
import psycopg

from utils.logger import setup_logger


class DatabaseConnector:
    """
    Class to interact with the database.

    db_credentials must be provided in the form of a dict. Example:
        db_credentials = {
            "host": "localhost",
            "port": "5432",
            "database": "example_data",
            "user": "example_user",
            "password": "example_user_password"
        }
    """

    def __init__(self, db_credentials: dict, logger=None):
        self.conninfo_str = f"""
        host={db_credentials["host"]}
        port={db_credentials["port"]}
        dbname={db_credentials["database"]}
        user={db_credentials["user"]}
        password={db_credentials["password"]}
        """

        self.logger = logger if logger else setup_logger(name="db_client_logger")

    def send_request(self, sql: str, values: tuple = ()) -> str:
        """Sends sql query to the db and return the status message."""
        try:
            with psycopg.connect(self.conninfo_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, values)
                    status = cur.statusmessage
                    conn.commit()

        except psycopg.Error as e:
            self.logger.error(e)
            self.logger.error("Unable to execute command.")
            return ""

        return status

    def send_query(self, sql: str, values: tuple = ()) -> str:
        """Sends sql query to the db and return all data queried."""
        try:
            with psycopg.connect(self.conninfo_str) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, values)
                    data = cur.fetchall()
                    conn.commit()

        except psycopg.Error as e:
            self.logger.error(e)
            self.logger.error("Unable to execute command.")
            return ""

        return data

    def insert_candles(self, table_name: str, candle_data: pd.DataFrame):
        # Saves candle_data on a temp csv file.
        try:
            temp_file = f"./temp_{table_name}.csv"
            candle_data.to_csv(temp_file, index=False, header=False)
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Unable to save data as temp csv.")
            return

        # Reads the temp csv file and inserts the data into the table_name
        try:
            with psycopg.connect(self.conninfo_str) as conn:
                with conn.cursor() as cur:
                    # Get the table columns from the table
                    cur.execute(
                        f"""SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                        AND table_name   = '{table_name}'
                        ORDER BY ordinal_position;"""
                    )
                    table_columns = cur.fetchall()
                    table_columns.pop(0)
                    table_columns = [x[0] for x in table_columns]
                    table_columns_str = ", ".join(table_columns)

                    # Create a staging table
                    cur.execute(
                        f"""CREATE TEMPORARY TABLE staging_table_{table_name}
                        (LIKE {table_name}
                        INCLUDING defaults
                        INCLUDING constraints
                        INCLUDING indexes);"""
                    )
                    if cur.statusmessage != "CREATE TABLE":
                        self.logger.error(cur.statusmessage)
                        self.logger.error("Unable to create staging table.")
                        return

                    # Copy the data from the csv to the staging table
                    with open(temp_file, "r") as f:
                        with cur.copy(
                            f"""COPY staging_table_{table_name} ({table_columns_str}) FROM STDIN WITH (FORMAT CSV);
                            """
                        ) as copy:
                            while data := f.read(8192):
                                copy.write(data)

                        print(cur.statusmessage)

                    # Insert data from staging table to table_name
                    do_update_columns_str = ", ".join([f"{col} = excluded.{col}" for col in table_columns])
                    cur.execute(
                        f"""INSERT INTO {table_name} ({table_columns_str})
                        SELECT {table_columns_str}
                        FROM staging_table_{table_name}
                        ON CONFLICT (date, market)
                        DO UPDATE SET {do_update_columns_str};
                        """
                    )
                    self.logger.debug(cur.statusmessage)

                    # Delete temp csv file
                    os.remove(f"./temp_{table_name}.csv")

        except psycopg.Error as e:
            self.logger.error(e)
            self.logger.error("Unable to execute command.")
