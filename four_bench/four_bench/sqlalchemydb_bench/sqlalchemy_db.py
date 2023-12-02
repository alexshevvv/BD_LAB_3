from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    REAL,
    DateTime,
    Table,
    MetaData,
)
import datetime
from tqdm import tqdm
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from four_bench.base_class import DatabaseForBench
from four_bench.config import Config
from sqlalchemy import text

Base = declarative_base()

converter_types = {
    "INTEGER": Integer,
    "TEXT": String,
    "FLOAT": Float,
    "REAL": REAL,
    "DATETIME": DateTime,
    "FLOAT8": Float,
}

converter_python_types = {
    "INTEGER": int,
    "TEXT": str,
    "FLOAT": float,
    "REAL": float,
    "DATETIME": datetime.datetime.strptime,
    "FLOAT8": float,
}


class SQLAlchemyDB(DatabaseForBench):
    """SQLAlchemy database"""

    def __init__(self, **kwargs) -> None:
        """Init SQLAlchemyDB"""
        super().__init__(**kwargs)
        self.csv_file = kwargs.get("csv_file")
        self.delimetr = kwargs.get("delimetr", ",")
        create_new_db = kwargs.get("create_new_db", True)  # type: bool

        self.engine = None
        self.session = None
        self.create()
        if create_new_db:
            self._fill_database()

    def create(self) -> None:
        """Create SQLAlchemyDB"""
        self.engine = create_engine("sqlite:///sqlalchemydb.db", echo=False)
        # self.engine = create_engine("sqlite:///sqlalchemydb.db", echo=False)

        self.session = sessionmaker(bind=self.engine)()
        coluns_to_create = []
        for column in Config.columns:
            splitted_column = column.split()
            column_name = splitted_column[0]
            column_type = splitted_column[1]
            if "PRIMARY KEY" in column_type.upper():
                primary_key = True
            else:
                primary_key = False
            coluns_to_create.append(
                Column(
                    column_name,
                    converter_types[column_type.upper()],
                    primary_key=primary_key,
                )
            )
        metadata = MetaData()
        self.trips = Table(
            "trips",
            metadata,
            *coluns_to_create,
        )
        metadata.create_all(self.engine)
        # self._fill_database()

    def _fill_database(self) -> None:
        """Fill database

        Raises:
            ValueError: If column type is not supported
        """
        with open(self.csv_file, "r") as fp:
            next(fp)  # Пропускаем заголовки
            for line in tqdm(fp):
                line = line.split(self.delimetr)[:-1]
                if len(line) != len(Config.columns):
                    line.extend([None] * (len(Config.columns) - len(line)))
                row_data = dict()
                for i in range(len(line)):
                    column_name, column_type = Config.columns[i].split(" ")[:2]
                    if column_type == "DATETIME":
                        data = converter_python_types[column_type](
                            line[i], Config.date_time_format
                        )
                    else:
                        try:
                            data = (
                                converter_python_types[column_type](line[i])
                                if line[i] is not None
                                else None
                            )
                        except ValueError:
                            if column_type == "INTEGER":
                                data = int(float(line[i]))
                            elif len(line[i]) == 0:
                                data = None
                            else:
                                raise ValueError
                    row_data[column_name] = data

                self.session.execute(
                    self.trips.insert(),
                    row_data,
                )
        self.session.commit()

    def close(self) -> None:
        """Close SQLAlchemyDB"""
        self.session.close()

    def query(self, query: str) -> list:
        """Query SQLAlchemyDB

        Args:
            query (str): Query

        Returns:
            list: Result of query
        """
        return self.session.execute(text(query)).fetchall()
