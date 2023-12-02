from abc import abstractmethod, ABCMeta


class DatabaseForBench(metaclass=ABCMeta):
    """
    Abstract class for database
    """

    @abstractmethod
    def __init__(self, **kwargs):
        """
        Initialize database
        :param csv_file: csv file
        :param kwargs: arguments
        """

        # self.kwargs = kwargs

    @abstractmethod
    def create(self):
        """
        Create database
        :return:
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close database
        :return:
        """
        pass

    @abstractmethod
    def query(self, query: str):
        """
        Query database
        :param query: query string
        :return:
        """
        pass

    def __del__(self):
        """
        Delete database
        :return:
        """
        self.close()
