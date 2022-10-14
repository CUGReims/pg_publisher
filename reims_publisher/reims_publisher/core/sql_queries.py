def get_schemas_dependencies(schemas):
    joined_schemas = ", ".join(f"'{schema}'" for schema in schemas)
    return """
    WITH cte as (SELECT dependent_ns.nspname as dependent_schema,
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
    JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace)
    SELECT DISTINCT 'Current Schema', dependent_schema, 'dependent_view',
     dependent_view, 'source_schema', source_schema, 'source_table', source_table from CTE
    WHERE cte.dependent_schema <> cte.source_schema and cte.dependent_schema in ('schema1');
    """.format(
        joined_schemas
    )
