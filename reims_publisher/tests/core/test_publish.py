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


def test_publish_schema_success(src_conn_string, dst_conn_string, schema, dst_conn):
    from reims_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, schemas=["schema"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP SCHEMA schema CASCADE;")
