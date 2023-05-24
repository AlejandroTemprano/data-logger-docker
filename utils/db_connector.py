import psycopg


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

    def __init__(self, db_credentials):
        self.host = db_credentials["host"]
        self.port = db_credentials["port"]
        self.database = db_credentials["database"]
        self.user = db_credentials["user"]
        self.password = db_credentials["password"]

    def create_test_table():
        ...
