import pytest


@pytest.mark.usefixtures("schemas")
def test_schema_querier_get_schema(views, dst_conn):
    from reims_publisher.core.information_schema import SchemaQuerier

    dependencies = SchemaQuerier.get_dependant_schemas_objects(
        dst_conn, ["schema2", "schema1"]
    )
    assert len(dependencies) == 1
