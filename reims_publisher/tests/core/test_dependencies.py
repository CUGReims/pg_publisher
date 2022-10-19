import pytest


@pytest.mark.usefixtures("views")
def test_schema_querier_get_schema_dependencies(views, dst_conn):
    from reims_publisher.core.information_schema import SchemaQuerier

    dependencies = SchemaQuerier.get_dependant_schemas_objects(
        dst_conn, ["schema2", "schema1"]
    )
    assert len(dependencies) == 2


@pytest.mark.usefixtures("views")
def test_schema_querier_get_table_dependencies(views, dst_constraint_table, dst_conn):
    from reims_publisher.core.information_schema import SchemaQuerier

    dependencies = SchemaQuerier.get_dependant_tables_objects(
        dst_conn, ["schema2.table1"]
    )
    assert dependencies['views'][0]['dependent_schema'] == 'schema1'
    assert dependencies['views'][0]['dependent_view'] == 'view2'

    assert dependencies['tables'][0]['dependent_table'] == 'schema1.table_with_fk1'
