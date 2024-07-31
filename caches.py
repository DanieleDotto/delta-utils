import logging
from logging import Logger

import duckdb
import pyarrow as pa
from deltalake import DeltaTable


class ArrowDeltaSimpleCache:
    """
    ArrowDeltaSimpleCache is a simple in-memory cache for DeltaTables.
    It uses DuckDB as the query engine to execute SQL queries on the cached
    DeltaTable. The cache is updated whenever a new version of the DeltaTable
    is available. It is designed to be used with a single DeltaTable, and it
    caches the entire DeltaTable as a pyarrow Table.

    **It must be initialized after the construction.**

    Parameters
    ----------
    deltatable : DeltaTable
        The DeltaTable object to cache.
    logger : Logger
        The logger object to log messages.

    Properties
    ----------
    version : int
        The version of the cached DeltaTable.
    table : pa.Table
        The cached DeltaTable as a pyarrow Table.

    Methods
    -------
    init()
        Initializes the cache.
    clear()
        Clears the cache.
    refresh()
        Refreshes the cache. If a new version of the DeltaTable is available,
        it updates the cache.
    query(query) -> pa.Table
        Executes the query on the cached DeltaTable and returns the result as
        a pyarrow Table. If a new version of the DeltaTable is available, it
        updates the cache before executing the query.
    """
    def __init__(self, deltatable: DeltaTable, logger: Logger):
        self.__logger = logger
        self.__delta_table = deltatable
        self.__current_version = None
        self.__cached_table = None
        self.__con = duckdb.connect()

    def __check_new_version_available(self) -> bool:
        self.__logger.log(logging.INFO,
                          "Checking for new version of the delta table...")
        current_version = self.__delta_table.version()
        cached_version = self.version
        if current_version != cached_version:
            self.__logger.log(logging.INFO,
                              f"New version available: {current_version}")
            return True
        self.__logger.log(logging.INFO,
                          "Cached version is up to date.")
        return False

    def __update_table_cache(self) -> None:
        self.__logger.log(logging.INFO, "Updating the cache...")
        pa_table = self.__delta_table.to_pyarrow_table()  # noqa: F841
        self.__cached_table = self.__con.execute(
            "SELECT * FROM pa_table"
        ).fetch_arrow_table()
        self.__con.register("pa_table", self.__cached_table)
        self.__logger.log(logging.INFO, "Cache updated.")

    @property
    def version(self) -> int:
        """
        The version of the cached DeltaTable.
        """
        return self.__current_version

    @property
    def table(self) -> pa.Table:
        """
        The cached DeltaTable as a pyarrow Table.
        """
        self.__logger.log(logging.INFO, "Retrieving the cached deltatable...")
        return self.__cached_table

    def init(self) -> None:
        """
        Initializes the cache.
        It must be called after the construction.
        """
        self.__logger.log(logging.INFO, "Initializing the cache...")
        self.refresh()
        self.__logger.log(logging.INFO, "Cache initialized successfully.")

    def clear(self) -> None:
        """
        Clears the cache.
        """
        self.__logger.log(logging.INFO, "Clearing the cache...")
        self.__cached_table = None
        self.__con.unregister("pa_table")
        self.__logger.log(logging.INFO, "Cache cleared.")

    def refresh(self) -> None:
        """
        Refreshes the cache.
        If a new version of the DeltaTable is available, it updates the cache.
        """
        if self.__check_new_version_available():
            self.__update_table_cache()

    def query(self, query: str) -> pa.Table:
        """
        Executes the query on the cached DeltaTable and returns the result as
        a pyarrow Table. If a new version of the DeltaTable is available, it
        updates the cache before executing the query.
        """
        self.refresh()
        self.__logger.log(logging.INFO, f"Executing query: {query}")
        arrow_res = self.__con.execute(query).fetch_arrow_table()
        return arrow_res
