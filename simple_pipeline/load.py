"""Helpers for loading a pandas DataFrame to bit.io"""

import csv
from io import StringIO
import logging

from sqlalchemy import create_engine


logger = logging.getLogger(__name__)


def _psql_insert_copy(table, conn, keys, data_iter):
    """DataFrame.to_sql Insertion method using PostgreSQL COPY FROM.

    Adapted for bit.io from pandas docs: https://pandas.pydata.org/docs/

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(f'"{k}"' for k in keys)
        table_name = f'"{table.schema}"."{table.name}"'
        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)


def _table_exists(engine, schema, table):
    """Checks for existence of a table.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    schema : str
        The destination schema.
    table : str
        The destination table.

    Return
    ----------
    boolean
    """
    with engine.connect() as conn:
        sql = '''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s;
        '''
        return conn.execute(sql, schema, table).rowcount


def _truncate_table(engine, schema, table):
    """Truncates (deletes all data from) a table.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    schema : str
        The destination schema.
    table : str
        The destination table.
    """
    with engine.connect() as conn:
        sql = f'''
            TRUNCATE TABLE "{schema}"."{table}";
        '''
        conn.execute(sql)


def to_table(df, destination, pg_conn_string):
    """
    Loads a pandas DataFrame to a bit.io database.

    Parameters
    ----------
    df : pandas.DataFrame
    destination : str
        Fully qualified bit.io table name.
    pg_conn_string : str
        A bit.io PostgreSQL connection string including credentials.
    """
    # Validation and setup
    if pg_conn_string is None:
        raise ValueError("You must specify a PG connection string.")
    schema, table = destination.split(".")
    engine = create_engine(pg_conn_string)

    # Check if table exists and set load type accordingly
    if _table_exists(engine, schema, table):
        _truncate_table(engine, schema, table)
        if_exists = 'append'
    else:
        if_exists = 'fail'

    with engine.connect() as conn:
        # 10 minute upload limit
        conn.execute("SET statement_timeout = 600000;")
        df.to_sql(
            table,
            conn,
            schema,
            if_exists=if_exists,
            index=False,
            method=_psql_insert_copy)
