import os

import pytest


@pytest.mark.usefixtures("src_table")
def test_publish_schema_success(src_conn_string, dst_conn_string, src_table, dst_conn):
    from pg_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, schemas=["schema"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP SCHEMA schema CASCADE;")


@pytest.fixture
def dst_schema_not_owned(dst_conn):
    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA schema;")

    yield

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("DROP SCHEMA schema;")


@pytest.mark.usefixtures("src_table")
def test_publish_schema_not_owned(
    src_conn_string, some_user, src_table, dst_schema_not_owned, dst_conn
):
    from pg_publisher.core.publish import publish, PsqlOperationalError

    with pytest.raises(PsqlOperationalError) as excinfo:
        publish(src_conn_string, some_user, schemas=["schema"])

    assert "must be owner of schema schema" in str(excinfo.value)


@pytest.mark.usefixtures("dst_schema")
@pytest.mark.usefixtures("src_table")
def test_publish_table_success(
    src_conn_string, dst_conn_string, src_table, dst_schema, dst_conn
):
    from pg_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, tables=["schema.table"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP table schema.table CASCADE;")


@pytest.mark.usefixtures("src_view")
def test_publish_view_missing_schema(
    src_conn_string, dst_conn_string, src_table, dst_schema, dst_conn
):
    from pg_publisher.core.publish import publish, PsqlOperationalError

    with pytest.raises(PsqlOperationalError) as excinfo:
        publish(src_conn_string, dst_conn_string, views=["schema.view"])
    assert 'relation "schema.table" does not exist' in str(excinfo.value)


@pytest.mark.usefixtures("src_view")
def test_publish_view_success(
    src_conn_string, dst_conn_string, dst_table, src_table, dst_schema, dst_conn
):
    from pg_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, views=["schema.view"])
    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.view WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP VIEW schema.view CASCADE;")


@pytest.fixture
@pytest.mark.usefixtures("src_table", "dst_table")
def user_not_owner(src_conn):
    with src_conn:
        with src_conn.cursor() as cursor:
            cursor.execute("CREATE USER not_owner WITH PASSWORD 'not_owner';")
            cursor.execute("GRANT USAGE ON SCHEMA schema TO not_owner;")
            cursor.execute("GRANT SELECT ON TABLE schema.table TO not_owner;")

    yield "not_owner"

    with src_conn:
        with src_conn.cursor() as cursor:
            cursor.execute("DROP OWNED BY not_owner;")
            cursor.execute("DROP USER not_owner;")


@pytest.mark.usefixtures("src_table", "dst_table", "user_not_owner")
def test_publish_with_read_error(src_conn_string, dst_conn_string, dst_conn):
    """
    Test that psql does not commit transaction when pg_dump fails.
    """
    from pg_publisher.core.publish import publish, PsqlOperationalError

    src_conn_string = "host={SRC_PGHOST} dbname={SRC_PGDATABASE} user=not_owner password=not_owner".format(
        **os.environ
    )

    with pytest.raises(PsqlOperationalError) as excinfo:
        publish(src_conn_string, dst_conn_string, schemas=["schema"])
    assert 'permission denied for sequence table_id_seq' in str(excinfo.value)

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
