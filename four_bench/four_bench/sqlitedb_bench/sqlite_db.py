import sqlite3
from four_bench.base_class import DatabaseForBench
from four_bench.config import Config
from tqdm import tqdm


class SQLiteDB(DatabaseForBench):
    """SQLite database"""

    def __init__(self, **kwargs) -> None:
        """Init SQLiteDB"""
        super().__init__(**kwargs)
        self.csv_file = kwargs.get("csv_file")
        self.delimetr = kwargs.get("delimetr", ",")
        create_new_db = kwargs.get("create_new_db", True)  # type: bool
        
        self.dbname = kwargs.get("dbname", "database")
        self.conn = sqlite3.connect("sqlite.db")

        self.cursor = self.conn.cursor()
        if create_new_db:
            self.create()

    def create(self) -> None:
        """Create SQLiteDB"""

        self.cursor.execute(f"DROP TABLE IF EXISTS {self.dbname}")
        create_table_query = f"""CREATE TABLE IF NOT EXISTS trips (
            {' ,'.join(Config.columns)}
)
"""

        self.cursor.execute(create_table_query)
        with open(self.csv_file, "r") as fp:
            next(fp)  # Пропускаем заголовки
            for line in tqdm(fp):
                line = line.split(self.delimetr)[:-1]
                if len(line) != len(Config.columns):
                    line.extend([None] * (len(Config.columns) - len(line)))

                self.cursor.execute(
                    f"INSERT INTO trips VALUES ({','.join(['?']*len(line))})",
                    line,
                )
        
        self.conn.commit()

    def close(self) -> None:
        """Close SQLiteDB"""
        self.conn.close()

    def query(self, query: str) -> list:
        """Query SQLiteDB

        Args:
            query (str): Query

        Returns:
            list: Result of query
        """
        return self.cursor.execute(query).fetchall()
