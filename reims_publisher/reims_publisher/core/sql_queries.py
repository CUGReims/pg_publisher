def get_schemas_dependencies(schemas: [str]) -> [dict]:
    joined_schemas = ", ".join(f"'{schema}'" for schema in schemas)
    return """
    WITH cte as (
      SELECT dependent_ns.nspname as source_schema,
       dependent_view.relname as dependent_view,
       source_ns.nspname as dependent_schema,
       source_table.relname as dependent_table
      FROM pg_depend
      JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
      JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
      JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
      JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
      AND pg_depend.refobjsubid = pg_attribute.attnum
      JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
      JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
    )
    SELECT DISTINCT 'dependent_schema', dependent_schema, 'view',
      dependent_view, 'source_schema', source_schema, 'dependent_table', dependent_table,
      'dependent_schema_table', dependent_schema || '.' || dependent_table
    FROM cte
    WHERE cte.dependent_schema <> cte.source_schema and cte.dependent_schema in ({});
    """.format(
        joined_schemas
    )


def get_schemas_fk_constraints(schemas: [str]) -> [dict]:
    joined_schemas = ", ".join(f"'{schema}'" for schema in schemas)
    return """
    WITH cte as (
      SELECT la.attrelid::regclass AS source_schema_table,
        la.attname AS referencing_column,
        c.confrelid::regclass as schema_table,
        c.contype as constraint_type,
        c.connamespace::regnamespace as schemaname
      FROM pg_constraint AS c
      JOIN pg_index AS i ON i.indexrelid = c.conindid
      JOIN pg_attribute AS la ON la.attrelid = c.conrelid AND la.attnum = c.conkey[1]
      JOIN pg_attribute AS ra ON ra.attrelid = c.confrelid AND ra.attnum = c.confkey[1]
      )
    SELECT 'source_schema', schemaname::varchar as schemaname,
    'dependent_schema_table', schema_table::varchar,
    'source_schema_table', source_schema_table::varchar, 'type_of_constraint',
    referencing_column::varchar, 'dependent_schema', split_part(schema_table::varchar, '.', 1)
    FROM cte
    WHERE cte.constraint_type = 'f' AND schemaname::varchar in ({})""".format(
        joined_schemas
    )


def get_schemas_fk_dependencies(schemas: [str]) -> [dict]:
    joined_schemas = ", ".join(f"'{schema}'" for schema in schemas)
    return """
    WITH cte as (
      SELECT la.attrelid::regclass AS source_schema_table,
        la.attname AS referencing_column,
        c.confrelid::regclass as schema_table,
        c.contype as constraint_type,
        c.connamespace::regnamespace as schemaname
      FROM pg_constraint AS c
      JOIN pg_index AS i ON i.indexrelid = c.conindid
      JOIN pg_attribute AS la ON la.attrelid = c.conrelid AND la.attnum = c.conkey[1]
      JOIN pg_attribute AS ra ON ra.attrelid = c.confrelid AND ra.attnum = c.confkey[1]
      )
    SELECT 'source_schema', schemaname::varchar, 'source_schema_table', schema_table::varchar,
    'dependent_schema_table', source_schema_table::varchar, 'type_of_constraint',
    referencing_column::varchar, 'dependent_schema', split_part(schema_table::varchar, '.', 1)
    FROM cte
    WHERE cte.constraint_type = 'f' AND schemaname::varchar in ({})""".format(
        joined_schemas
    )


def get_tables_fk_dependencies(tables: [str]) -> [dict]:
    joined_tables = ", ".join(f"'{table}'" for table in tables)
    return """
    WITH cte as (
      SELECT la.attrelid::regclass AS source_schema_table,
        la.attname AS referencing_column,
        c.confrelid::regclass as schema_table,
        c.contype as constraint_type,
        c.connamespace::regnamespace as schemaname
      FROM pg_constraint AS c
      JOIN pg_index AS i ON i.indexrelid = c.conindid
      JOIN pg_attribute AS la ON la.attrelid = c.conrelid AND la.attnum = c.conkey[1]
      JOIN pg_attribute AS ra ON ra.attrelid = c.confrelid AND ra.attnum = c.confkey[1]
      )
    SELECT 'source_schema', schemaname::varchar, 'dependent_schema_table', schema_table::varchar,
    'source_schema_table', source_schema_table::varchar, 'type_of_constraint',
     referencing_column::varchar, 'dependent_schema',
     split_part(schema_table::varchar, '.', 1)
    FROM cte
    WHERE cte.constraint_type = 'f' AND cte.source_schema_table::varchar in ({})""".format(
        joined_tables
    )


def get_tables_view_dependencies(tables: [str]) -> [dict]:
    joined_tables = ", ".join(f"'{table}'" for table in tables)
    return """WITH cte as (
      SELECT dependent_ns.nspname as dependent_schema,
       dependent_view.relname as dependent_view,
       source_ns.nspname as source_schema,
       source_table.relname as dependent_table,
       concat(source_ns.nspname, '.', source_table.relname) as schema_table_name
      FROM pg_depend
      JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
      JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
      JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
      JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
      AND pg_depend.refobjsubid = pg_attribute.attnum
      JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
      JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
      )
      SELECT DISTINCT 'dependent_schema', dependent_schema, 'view',
      dependent_view, 'source_schema', source_schema, 'dependent_table', dependent_table,
      'dependent_schema_table', schema_table_name
    FROM cte
    WHERE cte.dependent_schema <> cte.source_schema and cte.schema_table_name in ({});
    """.format(
        joined_tables
    )


def get_view_elements(views: str) -> [dict]:
    schema_name = views[0].split(".")[0]
    joined_views = ", ".join(f"'{view.split('.')[1]}'" for view in views)
    return """
       WITH RECURSIVE view_dependencies AS (
  SELECT
    'dependent_schema' AS column_name,
    table_schema,
    'source_schema' AS column_name,
    '{}' AS source_schema,
    'dependent_table' AS column_name,
    table_name,
    'dependent_schema_table' AS column_name,
    table_schema || '.' || table_name AS dependent_schema_table,
    'view' AS column_name,
    view_name

  FROM
    information_schema.view_table_usage
  WHERE
    view_schema = '{}' AND
    view_name in ({}) -- Replace with your actual schema and view name
  UNION
  SELECT
    'dependent_schema_table' AS column_name,
    vt.table_schema,
    'source_schema' AS column_name,
    'test_pub_vue' AS source_schema,
    'dependent_table' AS column_name,
    vt.table_name,
    'dependent_schema_table' AS column_name,
    vt.table_schema || '.' || vt.table_name AS dependent_schema_table,
    'view' AS column_name,
    vt.view_name
  FROM
    view_dependencies vd
    JOIN information_schema.view_table_usage vt ON vd.table_schema = vt.view_schema AND vd.table_name = vt.view_name
)SELECT * FROM
  view_dependencies;


        """.format(
        schema_name, schema_name, joined_views
    )
