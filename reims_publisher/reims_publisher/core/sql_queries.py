def get_schemas_dependencies(schemas: [str]) -> [dict]:
    joined_schemas = ", ".join(f"'{schema}'" for schema in schemas)
    return """
    WITH cte as (
      SELECT dependent_ns.nspname as dependent_schema,
       dependent_view.relname as dependent_view,
       source_ns.nspname as source_schema,
       source_table.relname as source_table
      FROM pg_depend
      JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
      JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
      JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
      JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
      AND pg_depend.refobjsubid = pg_attribute.attnum
      JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
      JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
    )
    SELECT DISTINCT 'dependent_schema', dependent_schema, 'dependent_view',
      dependent_view, 'source_schema', source_schema, 'source_table', source_table
    FROM cte
    WHERE cte.dependent_schema <> cte.source_schema and cte.dependent_schema in ({});
    """.format(
        joined_schemas
    )


def get_tables_fk_dependencies(tables: [str]) -> [dict]:
    joined_tables = ", ".join(f"'{table}'" for table in tables)
    return """
    WITH cte as (
      SELECT la.attrelid::regclass AS referencing_table,
        la.attname AS referencing_column,
        c.confrelid::regclass as schema_table,
        c.contype as constraint_type
      FROM pg_constraint AS c
      JOIN pg_index AS i ON i.indexrelid = c.conindid
      JOIN pg_attribute AS la ON la.attrelid = c.conrelid AND la.attnum = c.conkey[1]
      JOIN pg_attribute AS ra ON ra.attrelid = c.confrelid AND ra.attnum = c.confkey[1]
      )
    SELECT 'schema_table', schema_table::varchar, 'dependent_table', referencing_table::varchar,
    'type_of_constraint', referencing_column::varchar
    FROM cte
    WHERE cte.constraint_type = 'f' AND cte.schema_table::varchar in ({})""".format(
        joined_tables
    )


def get_tables_view_dependencies(tables: [str]) -> [dict]:
    joined_tables = ", ".join(f"'{table}'" for table in tables)
    return """    WITH cte as (
      SELECT dependent_ns.nspname as dependent_schema,
       dependent_view.relname as dependent_view,
       source_ns.nspname as source_schema,
       source_table.relname as source_table,
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
      SELECT DISTINCT 'dependent_schema', dependent_schema, 'dependent_view',
      dependent_view, 'source_schema', source_schema, 'source_table', source_table
    FROM cte
    WHERE cte.dependent_schema <> cte.source_schema and cte.schema_table_name in ({});
    """.format(
        joined_tables
    )
