import pytest


@pytest.mark.usefixtures("views")
def test_schema_querier_get_schema_dependencies(views, dst_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    dependencies = SchemaQuerier.get_dependant_schemas_objects(
        dst_conn, ["schema2", "schema1"]
    )
    views = dependencies["views"]
    constraints = dependencies["constraints"]
    assert len(constraints) == 0

    assert views == [
        {
            "dependent_schema": "schema1",
            "view": "view2",
            "source_schema": "schema2",
            "dependent_table": "table1",
            "dependent_schema_table": "schema2.table1",
        },
        {
            "dependent_schema": "schema2",
            "view": "view1",
            "source_schema": "schema1",
            "dependent_table": "table1",
            "dependent_schema_table": "schema1.table1",
        },
    ]


@pytest.mark.usefixtures("views")
def test_schema_querier_get_table_constraint_dep(views, dst_constraint_table, dst_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    dependencies = SchemaQuerier.get_dependant_tables_objects(
        dst_conn, ["schema1.table_with_fk1"]
    )
    assert dependencies["constraints"][0]["dependent_schema_table"] == "schema2.table1"


@pytest.mark.usefixtures("views")
def test_schema_querier_get_table_view_dep(views, dst_constraint_table, dst_conn):
    from pg_publisher.core.information_schema import SchemaQuerier

    dependencies = SchemaQuerier.get_dependant_tables_objects(
        dst_conn, ["schema2.table1"]
    )
    assert dependencies["views"][0]["dependent_schema"] == "schema1"
    assert dependencies["views"][0]["view"] == "view2"
