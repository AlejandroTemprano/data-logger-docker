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
