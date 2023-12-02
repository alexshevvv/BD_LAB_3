import psycopg2
from four_bench.base_class import DatabaseForBench
from tqdm import tqdm
from psycopg2.extensions import cursor, connection

# from psycopg2 import _T_conn
from four_bench.config import Config
import os
from psycopg2.errors import InvalidTextRepresentation, InFailedSqlTransaction


class PsycopgDB(DatabaseForBench):
    """Psycopg2 database"""

    def __init__(self, **kwargs) -> None:
        """Init Psycopg2DB"""
        super().__init__(**kwargs)
        self.csv_file = kwargs.get("csv_file")
        self.delimetr = kwargs.get("delimetr", ",")
        self.dbname = kwargs.get("dbname", None)  # type: str
        create_new_db = kwargs.get("create_new_db", True)  # type: bool

        if self.dbname is None:
            self.dbname = os.getenv("POSTGRES_DB", "postgres")

        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        host = os.getenv("POSTRGRES_DB_HOST", "127.0.0.1")
        port = os.getenv("POSTRGRES_DB_PORT", 5432)

        self.params = {
            "dbname": self.dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }
        self.conn = psycopg2.connect(**self.params)  # type: connection
        self.curs: cursor = self.conn.cursor()
       
        self.create()

    def create(self) -> None:
        """Create Psycopg2DB"""

        self.curs.execute(f"DROP TABLE IF EXISTS {self.dbname};")
        self.conn.commit()

        query_for_create_table = (
            f"CREATE TABLE {self.dbname} ({', '.join(Config.columns)})".replace(
                "DATETIME", "TIMESTAMP"
            )
        )

        self.curs.execute(query_for_create_table)
        with open(self.csv_file, "r", encoding="utf-8") as fp:
            self.curs.copy_expert(f"COPY {self.dbname} FROM STDIN WITH CSV HEADER", fp)
        for column_definition in Config.columns:
            column_name, new_type = column_definition.split()[:2]
            if "DATETIME" == new_type:
                new_type = "TIMESTAMP WITH TIME ZONE"
            alter_query = (
                f"ALTER TABLE {self.dbname} ALTER COLUMN {column_name} TYPE {new_type}"
            )
            self.curs.execute(alter_query)

        self.conn.commit()


    def query(self, query: str) -> list:
        """Query Psycopg2DB

        Args:
            query (str): Query

        Returns:
            list: Result of query
        """
        self.curs.execute(query)
        return self.curs.fetchall()

    def close(self) -> None:
        """Close Psycopg2DB"""
        self.curs.close()
        self.conn.close()
