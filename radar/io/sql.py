""" IO operations for SQL databases - currently Postgres specific
"""
import time
from typing import Callable, Union, Any
from io import StringIO
from functools import wraps
from itertools import chain

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as Connection

from ..common import log


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
            conn.cancel()
            raise err
        conn.commit()
        return out
    return wrapper


@try_sql
def df_to_psql(conn: Connection, table: str, df: pd.DataFrame) -> None:
    """ Insert a pandas DataFrame into a Postgres table using a
    StringIO intermediary CSV to COPY FRO
    Args:
        conn: psycopg2 connection
        table (str): postgres table name
        df (pandas.DataFrame): dataframe to insert
    Returns:
        None
    """
    cur = conn.cursor()
    f = StringIO()
    df.to_csv(f)
    f.seek(0)
    cols = f.readline().strip().split(',')
    cur.copy_from(f, table, sep=',', columns=cols)
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
    cmd = sql.SQL('CREATE TABLE {} (like {})').format(
        sql.Identifier(new_table), sql.Identifier(existing_table))
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
    cmd = sql.SQL('DROP TABLE {}').format(sql.Identifier(table))
    cur.execute(cmd)
    cur.close()


@try_sql
def upsert_from_table(conn, source_table, target_table, columns):
    """ Copy between tables and upsert all columns.
    Currently potentially unsafe - doesn't sanitize table or
    column names - should be fixed once psycopg2 2.8 comes out
    """
    columns = [c.lower() for c in columns]
    cur = conn.cursor()
    cmd_string = (
        'INSERT INTO {}'
        ' SELECT * FROM {}'
        ' ON CONFLICT (userid, time) DO UPDATE SET ')
    cmd_string += ', '.join(['{} = {}' for c in columns])
    """  # Should work in psycopg2 2.8
    cmd = sql.SQL(cmd_string).format(
                  sql.Identifier(target_table), sql.Identifier(source_table),
                  *list(chain.from_iterable((
                    (sql.Identifier(c), sql.Identifier('EXCLUDED', c))
                    for c in columns))))
    """
    cmd = cmd_string.format(
        target_table, source_table,
        *list(chain.from_iterable((
            (c, 'EXCLUDED.' + c)) for c in columns)))
    cur.execute(cmd)


def df_upsert_psql(conn: Connection, table: str, df: pd.DataFrame) -> None:
    """ Upsert a Dataframe into a Postgres table through:
    DataFrame -> StringIO -> Temp table -> table
    Args:
        conn: psycopg2 connection
        table (str): Table to upsert data into
        df (pd.DataFrame): Dataframe containing data to upsert from
    """
    columns = [df.index.name] + list(df.columns)
    curr_time = str(time.time()).replace('.', '_')
    tmp_table = 'staging_' + table + '_' + curr_time
    create_table_like(conn, tmp_table, table)
    df_to_psql(conn, tmp_table, df)
    upsert_from_table(conn, tmp_table, table, columns)
    drop_table(conn, tmp_table)
