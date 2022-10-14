import pytest


@pytest.mark.usefixtures("schemas")
def test_schema_querier_get_schema(schemas, src_conn):
    from reims_publisher.core.information_schema import SchemaQuerier

    existing_schemas = SchemaQuerier.get_schemas(src_conn)
    assert len(existing_schemas) == 3


@pytest.mark.usefixtures("tables")
def test_schema_querier_get_tables_from_table(tables, src_conn):
    from reims_publisher.core.information_schema import SchemaQuerier

    existing_tables = SchemaQuerier.get_tables_from_schema(src_conn, "schema1")
    assert len(existing_tables) == 2


@pytest.mark.usefixtures("views")
def test_schema_querier_get_views_from_schema(views, src_conn):
    from reims_publisher.core.information_schema import SchemaQuerier

    existing_views = SchemaQuerier.get_views_from_schema(src_conn, "schema1")
    assert len(existing_views) == 2
