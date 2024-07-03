import snowflake.connector
from airflow.models import Variable


class SnowflakeConnector:
    def __init__(self):
        self.user = Variable.get("SNOW_USER")
        self.password = Variable.get("SNOW_PASSWORD")
        self.account = Variable.get("SNOW_ACCOUNT")
        self.warehouse = Variable.get("SNOW_WAREHOUSE")
        self.database = Variable.get("SNOW_DATABASE_PIPEDRIVE")
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
            )
            self.cursor = self.connection.cursor()
            print("Connected to Snowflake database successfully!")
        except Exception as e:
            print("Error connecting to Snowflake database:", str(e))

    def execute_query(self, query):
        try:
            print(query)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            # self.close_connection()
            return results
        except Exception as e:
            print(e)
            print("Error executing query:", str(e))

    def close_connection(self):
        try:
            if self.connection:
                self.connection.close()
                print("Connection to Snowflake database closed.")
        except Exception as e:
            print("Error closing connection:", str(e))
