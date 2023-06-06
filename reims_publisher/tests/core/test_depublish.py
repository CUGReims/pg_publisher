import pytest


@pytest.mark.usefixtures("dst_schema")
def test_publish_schema_success(dst_conn_string, dst_table, dst_conn):
    from reims_publisher.core.depublish import depublish

    depublish(dst_conn_string, schemas=["schema"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute(
                """SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = 'schema'
            );"""
            )
            row = cursor.fetchone()
            assert row == (False,)


@pytest.mark.usefixtures("dst_schema")
@pytest.mark.usefixtures("dst_table")
def test_depublish_table_success(
    dst_conn_string, dst_table, dst_schema, dst_conn
):
    from reims_publisher.core.depublish import depublish

    depublish(dst_conn_string, tables=["schema.table"])

    with dst_conn:
        with dst_conn.cursor() as cursor:
            cursor.execute(
                """SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = 'schema'
            AND table_name = 'table'
            );"""
            )
            row = cursor.fetchone()
            assert row == (False,)
