#!/usr/bin/env python
"""
Module for connection with a PostgreSQL Database .
"""
from typing import Union
import psycopg2                                     # type: ignore
import pandas as pd                                 # type: ignore
from modules.errors import ConnectionDBError

class Database():
    """
    PostgreSQL Database class.
    """
    #pylint: disable=too-many-arguments
    def __init__(
        self,
        host : str,
        database_name : str,
        port : str,
        username : str,
        password : str,
        ):
        self.host = host
        self.database_name = database_name
        self.port = port
        self.username = username
        self.password = password
        self._conn = None
        self._counter = 0

    def connect(self):
        """ Connect to a Postgres database."""
        if self._conn is None:
            try:
                self._conn = psycopg2.connect(
                    host=self.host,
                    database=self.database_name,
                    port=self.port,
                    user=self.username,
                    password=self.password
                )
            except psycopg2.DatabaseError as exc:
                self.disconnect()
                raise ConnectionDBError() from exc

    def disconnect(self):
        """ Disconnect to a Postgres database."""
        if self._conn is not None:
            self._conn.close()

    def __cursor_counter(self) -> str:
        """ Counts cursors so they do not overlap."""
        self._counter += 1
        return str(self._counter)

    def execute_sql(self, sql : str) -> Union[pd.DataFrame, None]:
        """ Executes sql on the Postgres database."""
        data = None
        if self._conn is not None:
            try:
                cur = self._conn.cursor(self.__cursor_counter())
                cur.execute(sql)
                data = cur.fetchall()
                cur.close()
            #pylint disable=broad-except
            except (Exception, psycopg2.DatabaseError) as exc:
                if not cur.closed:
                    cur.close()
                self.disconnect()
                raise ConnectionDBError("Failed to execute sql") from exc
        return data

    def closed(self):
        """Check if the connection is closed."""
        return self._conn.closed
