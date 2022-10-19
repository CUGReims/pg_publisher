import pytest


@pytest.mark.usefixtures("src_table")
def test_publish_schema_success(src_conn_string, dst_conn_string, src_table, dst_conn):
    from reims_publisher.core.publish import publish

    publish(src_conn_string, dst_conn_string, schemas=["schema"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute("SELECT data FROM schema.table WHERE num = 100;")
            row = cursor.fetchone()
            assert row == ("abc'def",)
            cursor.execute("DROP SCHEMA schema CASCADE;")
