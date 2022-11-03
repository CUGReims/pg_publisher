import pytest


@pytest.mark.usefixtures("src_table")
def test_publish_schema_success(src_conn_string, dst_conn_string, src_table, dst_conn):
    from reims_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, "/tmp/nicelog.log", schemas=["schema"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP SCHEMA schema CASCADE;")


@pytest.mark.usefixtures("dst_schema")
@pytest.mark.usefixtures("src_table")
def test_publish_table_success(
    src_conn_string, dst_conn_string, src_table, dst_schema, dst_conn, log_file
):
    from reims_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, log_file, tables=["schema.table"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP table schema.table CASCADE;")
