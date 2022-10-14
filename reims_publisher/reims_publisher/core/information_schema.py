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


class InformationSchemaChecker:
    def __init__(self, src_conn: object, dst_conn: object):
        """
        :param src_conn_string: source database connection string
        :param dst_conn_string: destination database connection string
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
