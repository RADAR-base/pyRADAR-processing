""" IO operations for SQL databases - currently Postgres specific
Currently unsafe / doesn't sanatize inputs. Should work in psycopg2 2.8
"""
import time
from typing import Callable, Union, Any, List
from io import StringIO
from functools import wraps
from itertools import chain

import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.pool import AbstractConnectionPool

import pandas as pd
from ..common import log


class PostgresIOPool():
    """ Class version of module
    """
    def __init__(self, pool: AbstractConnectionPool):
        self.pool = pool

    def df_to_psql(self, table: str, df: pd.DataFrame):
        """ Insert a pandas DataFrame into a Postgres table using a
        StringIO intermediary CSV to COPY FRO
        Args:
            table (str): postgres table name
            df (pandas.DataFrame): dataframe to insert
        Returns:
            None
        """
        conn = self.pool.getconn()
        return df_to_psql(conn, table, df)

    def create_table_like(self, new_table: str, existing_table: str):
        """ Create an SQL table based on another existing table
        Args:
            new_table (str): The name of the table being created
            existing_table (str): The name of the table to copy
        Returns:
            None
        """
        conn = self.pool.getconn()
        return create_table_like(conn, new_table, existing_table)

    def drop_table(self, table: str):
        """ Drop a table (dynamic name)
        Args:
            table (str): table name
        """
        conn = self.pool.getconnO()
        drop_table(conn, table)

    def upsert_from_table(self, source_table: str,
                          target_table: str, columns: List[str],
                          constraints: List[str] = ('userid', 'time')) -> None:
        """ Copy between tables and upsert all columns.
        Currently potentially unsafe - doesn't sanitize table or
        column names - should be fixed once psycopg2 2.8 comes out
        Args:
            source_table (str): Name of table to copy from
            target_table (str): Name of table to copy into
            columns (list): List of column names
            constraints (list): List of UNIQUE CONSTRAINT
                columns in sql table
        """
        conn = self.pool.getconn()
        return upsert_from_table(conn, source_table,
                                 target_table, columns, constraints)

    def df_upsert_psql(self, table: str, df: pd.DataFrame) -> None:
        """ Upsert a Dataframe into a Postgres table through:
        DataFrame -> StringIO -> Temp table -> table
        Args:
            table (str): Table to upsert data into
            df (pd.DataFrame): Dataframe containing data to upsert from
        """
        conn = self.pool.getconn()
        df_upsert_psql(conn, table, df)


def try_sql(func: Callable[[Connection, Any], Any]) -> Callable:
    """ Wrap a function that executes an SQL query in try-except.
    Cancels the connection on error, commits on success.
    """
    @wraps(func)
    def wrapper(conn: Connection, *args, **kwargs) -> Union[Any, str]:
        """ Try-except wrapper. Returns func output on success"""
        try:
            out = func(conn, *args, **kwargs)
        except psycopg2.Error as err:
            log.error('SQL error with func %s, args %s', func, args)
            curs = conn.cursor()
            curs.execute("ROLLBACK")
            conn.commit()
            raise err
        conn.commit()
        return out
    return wrapper


@try_sql
def df_to_psql(conn: Connection, table: str, df: pd.DataFrame) -> None:
    """ Insert a pandas DataFrame into a Postgres table using a
    StringIO intermediary CSV to COPY FROM
    Args:
        conn: psycopg2 connection
        table (str): postgres table name
        df (pandas.DataFrame): dataframe to insert
    Returns:
        None
    """
    cur = conn.cursor()
    f = StringIO()
    df.to_csv(f, na_rep='\\N', sep=';')
    f.seek(0)
    cols = f.readline().strip().split(';')
    cur.copy_from(f, table, sep=';', columns=cols)
    cur.close()


@try_sql
def create_table_like(conn: Connection,
                      new_table: str,
                      existing_table: str):
    """ Create an SQL table based on another existing table
    Args:
        conn: psycopg2 connection to the postgres database
        new_table (str): The name of the table being created
        existing_table (str): The name of the table to copy
    Returns:
        None
    """
    cur = conn.cursor()
    cmd = (
        'CREATE TABLE {} (like {} '
        'INCLUDING DEFAULTS '
        'INCLUDING CONSTRAINTS '
        'INCLUDING INDEXES)').format(new_table, existing_table)
    cur.execute(cmd)
    cur.close()
    conn.commit()


@try_sql
def drop_table(conn: Connection, table: str):
    """ Drop a table (dynamic name)
    Args:
        conn: psycopg2 connection
        table (str): table name
    """
    cur = conn.cursor()
    cmd = 'DROP TABLE {}'.format(table)
    cur.execute(cmd)
    cur.close()


@try_sql
def upsert_from_table(conn, source_table: str, target_table: str,
                      columns: List[str],
                      constraints: List[str] = ('userid', 'time')) -> None:
    """ Copy between tables and upsert all columns.
    Currently potentially unsafe - doesn't sanitize table or
    column names - should be fixed once psycopg2 2.8 comes out
    Args:
        conn: psycopg2 connection
        source_table (str): Name of table to copy from
        target_table (str): Name of table to copy into
        columns (list): List of column names
        constraints (list): List of UNIQUE CONSTRAINT
            columns in sql table
    """
    columns = [c.lower() for c in columns if c not in ('userid', 'time')]
    cur = conn.cursor()
    cmd_string = (
        'INSERT INTO {}'
        ' SELECT * FROM {}'
        ' ON CONFLICT ( ' + ', '.join(['{}' for c in constraints]) + ')'
        ' DO UPDATE SET '
    )
    cmd_string += ', '.join(['{} = {}' for c in columns])
    cmd = cmd_string.format(
        target_table, source_table,
        *[c for c in constraints],
        *list(chain.from_iterable((
            (c, 'EXCLUDED.' + c)) for c in columns)))
    cur.execute(cmd)


def df_upsert_psql(conn: Connection, table: str, df: pd.DataFrame,
                   constraints: List[str] = ('userid', 'time')) -> None:
    """ Upsert a Dataframe into a Postgres table through:
    DataFrame -> StringIO -> Temp table -> table
    Args:
        conn: psycopg2 connection
        table (str): Table to upsert data into
        df (pd.DataFrame): Dataframe containing data to upsert from
    """
    df = df.reset_index()\
        .drop_duplicates(subset=constraints)\
        .set_index(df.index.name)
    columns = [df.index.name] + list(df.columns)
    curr_time = str(time.time()).replace('.', '_')
    tmp_table = table + '_staging_' + curr_time
    create_table_like(conn, tmp_table, table)
    try:
        df_to_psql(conn, tmp_table, df)
        upsert_from_table(conn, tmp_table, table, columns, constraints)
    finally:
        drop_table(conn, tmp_table)


def dask_upsert_psql(pool, table, ddf, constraints=('userid', 'time')):
    """ Upsert a dask dataframe into SQL table
    Args:
        pool: psycopg2 connection pool
        table (str): table name
        ddf (dask dataframe)
    Returns:
        Dask delayed object
    """
    def upsert_with_pool(df):
        conn = pool.getconn()
        try:
            df_upsert_psql(conn, table, df, constraints)
        finally:
            conn.close()
            pool.putconn(conn)
    return ddf.map_partitions(upsert_with_pool, meta=(None, None))
