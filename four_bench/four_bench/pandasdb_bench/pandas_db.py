import pandas as pd
from four_bench.base_class import DatabaseForBench
from sqlalchemy import create_engine
import os
from tqdm import tqdm


class PandasDB(DatabaseForBench):
    """Pandas database"""

    def __init__(self, **kwargs) -> None:
        """Init PandasDB"""
        super().__init__(**kwargs)
        self.csv_file = kwargs.get("csv_file")
        self.delimetr = kwargs.get("delimetr", ",")
        self.chunksize = kwargs.get("chunksize", 10000)  # Default chunk size
        create_new_db = kwargs.get("create_new_db", True)  # type: bool
        
        self.dbname = kwargs.get("dbname", "database")
        self.engine = None
        self.engine = create_engine("sqlite:///pandas.db")
        if create_new_db:
            self.create()

    def create(self) -> None:
        """Create PandasDB"""
        try:
            os.remove("pandas.db")
        except OSError:
            pass
       

        first_chunk = True
        for chunk in tqdm(
            pd.read_csv(
                self.csv_file, delimiter=self.delimetr, chunksize=self.chunksize
            )
        ):
            chunk.rename(columns={"Airport_fee": "Another_fee"}, inplace=True)
            if first_chunk:
                chunk.to_sql("trips", self.engine, index=False)
                first_chunk = False
            else:
                chunk.to_sql("trips", self.engine, index=False, if_exists="append")

    def close(self) -> None:
        """Close PandasDB"""
        self.engine.dispose()

    def query(self, query: str) -> pd.DataFrame:
        """Query PandasDB

        Args:
            query (str): Query

        Returns:
            pd.DataFrame: Result of query
        """

        return pd.read_sql(query, self.engine)
