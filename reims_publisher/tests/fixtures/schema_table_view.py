import pytest


@pytest.fixture
def schema(src_conn):
    with src_conn:
        with src_conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA schema;")
            cursor.execute("CREATE TABLE schema.table (id serial PRIMARY KEY, num integer, data varchar);")
            cursor.execute("INSERT INTO schema.table (num, data) VALUES (%s, %s);", (100, "abc'def"))
    yield
    with src_conn:
        with src_conn.cursor() as cursor:
            cursor.execute("DROP SCHEMA schema CASCADE;")


@pytest.fixture
def schemas(src_conn, dst_conn):
    for conn in [src_conn, dst_conn]:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA schema1;")
                cursor.execute("CREATE SCHEMA schema2;")
    yield
    for conn in [src_conn, dst_conn]:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA schema1 CASCADE;")
                cursor.execute("DROP SCHEMA schema2 CASCADE;")


@pytest.fixture
def tables(src_conn, dst_conn, schemas):
    for conn in [src_conn, dst_conn]:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE schema1.table1 (id serial PRIMARY KEY, num integer, data varchar);")
                cursor.execute("CREATE TABLE schema1.table2 (id serial PRIMARY KEY, num integer, data varchar);")
                cursor.execute("CREATE TABLE schema2.table1 (id serial PRIMARY KEY, num integer, data varchar);")
                cursor.execute("CREATE TABLE schema2.table2 (id serial PRIMARY KEY, num integer, data varchar);")
    yield
    for conn in [src_conn, dst_conn]:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP TABLE schema1.table1 CASCADE;")
                cursor.execute("DROP TABLE schema1.table2 CASCADE;")
                cursor.execute("DROP TABLE schema2.table1 CASCADE;")
                cursor.execute("DROP TABLE schema2.table2 CASCADE;")

@pytest.fixture
def views(src_conn, dst_conn, tables):
    for conn in [src_conn, dst_conn]:
        with conn:
            with conn.cursor() as cursor:
                # basic view
                cursor.execute("CREATE VIEW schema1.view1 as select id, num FROM schema1.table1;")
                # view pointing to another table in another schema
                cursor.execute("CREATE VIEW schema1.view2 as select id, num FROM schema2.table1;")
                # view pointing to another table in another schema built with different schemas
                cursor.execute("CREATE VIEW schema2.view1 as select id, num FROM schema1.table1 NATURAL JOIN schema2.table2;")
    yield
    for conn in [src_conn, dst_conn]:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP VIEW schema2.view1 CASCADE;")
                cursor.execute("DROP VIEW schema1.view2 CASCADE;")
                cursor.execute("DROP VIEW schema1.view1 CASCADE;")
