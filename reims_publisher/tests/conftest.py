import os
from time import sleep

import psycopg2
import pytest
from tests.fixtures.schema_table_view import *

def get_connection(conn_str):
    conn = None
    remaining_tries = 3
    current_try = 0
    while conn is None:
        try:
            conn = psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            if remaining_tries <= 0:
                raise
            sleep(1 * pow(2, current_try))
        remaining_tries -= 1
        current_try +=1
    return conn


@pytest.fixture(scope="session")
def src_conn_string():
    yield "host={SRC_PGHOST} dbname={SRC_PGDATABASE} user={SRC_PGUSER} password={SRC_PGPASSWORD}".format(**os.environ)


@pytest.fixture(scope="session")
def dst_conn_string():
    yield "host={DST_PGHOST} dbname={DST_PGDATABASE} user={DST_PGUSER} password={DST_PGPASSWORD}".format(**os.environ)


@pytest.fixture(scope="session")
def src_conn(src_conn_string):
    conn = get_connection(src_conn_string)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="session")
def dst_conn(dst_conn_string):
    conn = get_connection(dst_conn_string)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="function")
def src_transact(src_conn):
    try:
        yield src_conn
    finally:
        src_conn.rollback()


@pytest.fixture(scope="function")
def dst_transact(src_conn):
    try:
        yield dst_conn
    finally:
        dst_conn.rollback()
