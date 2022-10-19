from reims_publisher.core.sql_queries import get_schemas_dependencies


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
            schemas = cursor.fetchall()
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
            dict(zip(dependency[::2], dependency[1::2]))
            for dependency in dependencies
        ]
        return dependencies_dict


class InformationSchemaChecker:
    def __init__(self, src_conn: object, dst_conn: object):
        """
        :param src_conn: source database connection
        :param dst_conn: destination database connection
        """
        self.src_conn = src_conn
        self.dst_conn = dst_conn

    def get_src_schemas(self) -> [str]:
        return SchemaQuerier.get_schemas(self.src_conn)

    def get_dst_schemas(self) -> [str]:
        return SchemaQuerier.get_schemas(self.dst_conn)

    def get_src_tables_from_schema(self, schema_name) -> [str]:
        return SchemaQuerier.get_tables_from_schema(self.src_conn, schema_name)

    def get_dst_tables_from_schema(self, schema_name) -> [str]:
        return SchemaQuerier.get_tables_from_schema(self.dst_conn, schema_name)

    def get_src_views_from_schema(self, schema_name) -> [str]:
        return SchemaQuerier.get_views_from_schema(self.src_conn, schema_name)

    def get_dst_views_from_schema(self, schema_name) -> [str]:
        return SchemaQuerier.get_views_from_schema(self.dst_conn, schema_name)

    def get_dst_schemas_dependencies(self, schemas) -> [dict]:
        return SchemaQuerier.get_dependant_schemas_objects(schemas)
