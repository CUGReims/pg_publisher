from reims_publisher.core.sql_queries import (
    get_schemas_dependencies,
    get_tables_view_dependencies,
    get_tables_fk_dependencies,
)


class SchemaQuerier:
    @staticmethod
    def get_schemas(database_connection: object) -> [str]:
        """
        :param database_connection:
        :return: a list of existing schemas from the database
        """
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT(table_schema) from"
                " information_schema.tables WHERE"
                " table_schema not in ('pg_catalog, information_schema')"
            )
            schemas = [r[0] for r in cursor.fetchall()]
            return schemas

    @staticmethod
    def get_tables_from_schema(database_connection: object, schema_name: str) -> [str]:
        """
        :param database_connection:
        :param schema_name:
        :return: a list of existing tables in the schema
        """
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT(table_name) FROM"
                " information_schema.tables WHERE"
                " table_schema = '{schema}';".format(schema=schema_name)
            )
            tables = cursor.fetchall()
            return tables

    @staticmethod
    def get_views_from_schema(database_connection: object, schema_name: str) -> [str]:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name, view_definition FROM"
                " information_schema.views WHERE"
                " table_schema = '{schema}';".format(schema=schema_name)
            )
            views = cursor.fetchall()
            return views

    @staticmethod
    def get_materialized_views_from_schema(
        database_connection: object, schema_name: str
    ) -> [str]:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT schemaname, matviewname, definition FROM"
                " pg_matviews WHERE"
                " schemaname = '{schema}';".format(schema=schema_name)
            )
            mat_views = cursor.fetchall()
            return mat_views

    @staticmethod
    def get_dependant_schemas_objects(database_connection: object, schemas) -> [dict]:
        with database_connection.cursor() as cursor:
            cursor.execute(get_schemas_dependencies(schemas))
            dependencies = cursor.fetchall()

        dependencies_dict = [
            dict(zip(dependency[::2], dependency[1::2])) for dependency in dependencies
        ]
        return dependencies_dict

    @staticmethod
    def get_dependant_tables_objects(database_connection: object, tables) -> [dict]:
        with database_connection.cursor() as cursor:
            cursor.execute(get_tables_view_dependencies(tables))
            dependencies_views = cursor.fetchall()

        with database_connection.cursor() as cursor:
            cursor.execute(get_tables_fk_dependencies(tables))
            dependencies_fk = cursor.fetchall()

        dependencies_views_dict = [
            dict(zip(dependency[::2], dependency[1::2]))
            for dependency in dependencies_views
        ]
        dependencies_fk_dict = [
            dict(zip(dependency[::2], dependency[1::2]))
            for dependency in dependencies_fk
        ]

        return {"views": dependencies_views_dict, "tables": dependencies_fk_dict}
