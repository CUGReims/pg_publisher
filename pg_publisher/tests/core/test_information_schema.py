import pytest


@pytest.mark.usefixtures("schemas")
def test_schema_querier_get_schema(schemas, src_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    existing_schemas = SchemaQuerier.get_schemas(src_conn)
    assert existing_schemas == ["public"]


@pytest.mark.usefixtures("tables")
def test_schema_querier_get_tables_from_table(tables, src_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    existing_tables = SchemaQuerier.get_tables_from_schema(src_conn, "schema1")
    assert len(existing_tables) == 2


@pytest.mark.usefixtures("views")
def test_schema_querier_get_views_from_schema(views, src_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    existing_views = SchemaQuerier.get_views_from_schema(src_conn, "schema1")
    assert len(existing_views) == 2


@pytest.mark.usefixtures("views")
def test_schema_querier_get_schemas_with_views(views, src_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    existing_schemas = SchemaQuerier.get_schemas_with_views(src_conn)
    assert len(existing_schemas) == 3


@pytest.mark.usefixtures("materialized_views")
def test_schema_querier_get_materialized_views_from_schema(
    materialized_views, src_conn
):
    from pg_publisher.core.information_schema import SchemaQuerier

    existing_views = SchemaQuerier.get_materialized_views_from_schema(
        src_conn, "schema1"
    )
    assert len(existing_views) == 1


@pytest.mark.usefixtures("materialized_views")
def test_schema_querier_get_schemas_with_matviews(materialized_views, src_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    existing_schemas = SchemaQuerier.get_schemas_with_matviews(src_conn)
    assert len(existing_schemas) == 1
