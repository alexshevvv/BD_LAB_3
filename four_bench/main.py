import time
from tqdm import tqdm
import logging
from four_bench.pandasdb_bench import PandasDB
from four_bench.sqlitedb_bench import SQLiteDB
from four_bench.duckdb_bench import DuckDB
from four_bench.sqlalchemydb_bench import SQLAlchemyDB
from four_bench.psycopg2db_bench import PsycopgDB
from four_bench.utils import results_plot
import os
import json
import time

import numpy as np


db_types_converter = {
    "pandas": PandasDB,
    "duckdb": DuckDB,
    "sqlite": SQLiteDB,
    "psycopg2": PsycopgDB,
    "sqlalchemy": SQLAlchemyDB,
}

logger = logging.getLogger("db_bench")
logger.setLevel(logging.INFO)  # Установка общего уровня логирования

# Обработчик для записи в файл
fh = logging.FileHandler("db_bench.log")
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

# Обработчик для вывода в консоль
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_parameters(file: str = "config.json") -> dict:
    """Function for load and return parameters from json file

    Args:
        file (str, optional): file_name or file_path. Defaults to "config.json".

    Returns:
        dict: Ditionary of parameters
    """
    with open(
        os.path.join(os.path.dirname(__file__), file), "r", encoding="utf-8"
    ) as fp:
        parameters = json.load(fp)  # type: dict
    return parameters


def main():
    logger.info("Start Benchmark")

    parameters = get_parameters()

    # Получаем все нужные параметры с помощью метода get
    result_file = parameters.get("result_file", "result.csv")  # type: str
    destination_file = parameters.get("destination_file", "result.png")  # type: str
    data_file = parameters.get("data_file", None)  # type: str
    csv_file = os.path.join(
        os.path.dirname(__file__), "..", "data", data_file
    )  # type: str
    number_of_trials = parameters.get("number_of_trials", None)  # type: int
    test_types = parameters.get("db_types", None)  # type: list[str]
    all_queries = parameters.get("sql_queries", None)  # type: dict
    create_new_db = parameters.get("create_new_db", True)  # type: bool

    # Проверяем параметры на существование. Проверяются только обязательные параметры
    if data_file is None:
        logger.error("Data files not found")
        return

    if number_of_trials is None:
        logger.error("You should set up parameter `number_of_trials`")
        return

    if test_types is None:
        logger.error(
            f"You should setup `db_types` parameter. You can setup: {list(db_types_converter.keys())}"
        )
        return

    if all_queries is None:
        logger.error(f"You shoud setup queries for each type of databas/library")

    logger.info(f"Your tests: {test_types}")

    # Пересоздаем файл с результатами тестов
    # Важно файл сохранять после проведения теста, т.к. иначе
    # он будет просто удален при новом запуске.
    with open(result_file, "w", encoding="utf-8") as fp:
        fp.write("db,num_of_query,result\n")

    # Получем все базы данных (библиотеки для работы с базами данных)
    # из списка test_types, который является обязательным
    for db in test_types:
        db_name = db
        logger.info("Your sql-queries")

        # получаем все запросы из параметров, для конкретной базы данных/библиотеки
        sql_queries = all_queries.get(db_name, None)  # type: str
        # Если запросов нет, то выводим ошибку и покидаем функцию
        if sql_queries is None:
            logger.error(f"You should set up sql-queries for {db_name}")
            return

        # Выводим все запросы
        for i, query in enumerate(sql_queries, 1):
            logger.info(f"{i}. {query}")
        # Создаем объект базы данных для каждого из типов
        logger.info(f"Create database {db_name}")
        db = db_types_converter[db](
            csv_file=csv_file, dbname="trips", create_new_db=create_new_db
        )  # type: PsycopgDB | SQLAlchemyDB | DuckDB | SQLiteDB | PandasDB

        # Выполняем каждый запрос
        for query_num, query in enumerate(sql_queries, 1):
            logger.info(f"db: {db_name}, query: {query}")
            trials = []
            for _ in tqdm(range(number_of_trials)):
                start = time.perf_counter()
                db.query(query=query)
                end = time.perf_counter()
                trials.append(end - start)
            result = np.median(trials)  # Вычисляем медиану всех времен испытаний

            # Сохраняем результаты
            with open(result_file, "a", encoding="utf-8") as fp:
                fp.write(f"{db_name},{query_num},{result}\n")
            logger.info(f"Result {result}")
        del db

    # Строим график
    results_plot(
        source_file=result_file,
        destination_file=destination_file,
    )


if __name__ == "__main__":
    main()
