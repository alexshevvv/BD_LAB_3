import duckdb
from four_bench.base_class import DatabaseForBench


class DuckDB(DatabaseForBench):
    """DuckDB database"""

    def __init__(self, **kwargs) -> None:
        """Init DuckDB"""
        super().__init__(**kwargs)
        self.csv_file = kwargs.get("csv_file", None)
        self.delimetr = kwargs.get("delimetr", ",")
        create_new_db = kwargs.get("create_new_db", True)  # type: bool
        
        # self.db_name = kwargs.get("db_name", None)
        self.con = None
        self.cur = None
        self.dbname = kwargs.get("dbname", "database")
        self.con = duckdb.connect("duck.db")
        self.cur = self.con.cursor()
        if create_new_db:
            self.create()

    def create(self) -> None:
        """Create DuckDB"""
        
        self.cur.execute(f"DROP TABLE IF EXISTS {self.dbname}")
        self.cur.execute(
            f"CREATE TABLE {self.dbname} AS SELECT * FROM read_csv_auto('{self.csv_file}', sep='{self.delimetr}')"
            # f"CREATE TABLE trips AS SELECT * FROM read_csv_auto('{self.csv_file}')"
        )
        
        self.con.commit()

    def close(self) -> None:
        """Close DuckDB"""
        self.con.close()

    def query(self, query: str) -> list:
        """Query DuckDB

        Args:
            query (str): Query

        Returns:
            list: Result of query
        """
        return self.cur.execute(query).fetchall()
