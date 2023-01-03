from reims_publisher.core.sql_queries import (
    get_schemas_dependencies,
    get_tables_view_dependencies,
    get_tables_fk_dependencies,
    get_schemas_fk_dependencies,
)
import configparser
from pkg_resources import resource_filename

config = configparser.ConfigParser()
config.read(resource_filename("reims_publisher", "conf.ini"))


class SchemaQuerier:
    @staticmethod
    def get_schemas(database_connection: object) -> [str]:
        """
        :param database_connection:
        :return: a list of existing schemas from the database
        """
        s = config.get("DEFAULT", "IgnoredSchemas")

        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT(table_schema) from"
                " information_schema.tables WHERE"
                " table_schema not in ({})".format(
                    ",".join("'{0}'".format(x) for x in s.split(","))
                )
            )
            schemas = sorted([r[0] for r in cursor.fetchall()])
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
                " table_schema = '{schema}' AND"
                " table_type = 'BASE TABLE';".format(schema=schema_name)
            )
            tables = sorted(
                [
                    "{schema}.{table}".format(schema=schema_name, table=table[0])
                    for table in cursor.fetchall()
                ]
            )
            return tables

    @staticmethod
    def get_views_from_schema(database_connection: object, schema_name: str) -> [str]:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT (table_name) FROM"
                " information_schema.views WHERE"
                " table_schema = '{schema}';".format(schema=schema_name)
            )
            views = sorted(
                [
                    "{schema}.{table}".format(schema=schema_name, table=view[0])
                    for view in cursor.fetchall()
                ]
            )
            return views

    @staticmethod
    def get_materialized_views_from_schema(
        database_connection: object, schema_name: str
    ) -> [str]:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT matviewname FROM"
                " pg_matviews WHERE"
                " schemaname = '{schema}';".format(schema=schema_name)
            )
            mat_views = sorted(
                [
                    "{schema}.{mat_view}".format(
                        schema=schema_name, mat_view=mat_view[0]
                    )
                    for mat_view in cursor.fetchall()
                ]
            )
        return mat_views

    @staticmethod
    def get_dependant_schemas_objects(database_connection: object, schemas) -> [dict]:
        """
        :param database_connection:
        :param schemas:
        :return: Dict of views and constraint dependencies
        """
        with database_connection.cursor() as cursor:
            cursor.execute(get_schemas_dependencies(schemas))
            dependencies_view = cursor.fetchall()
            cursor.execute(get_schemas_fk_dependencies(schemas))
            dependencies_fk = cursor.fetchall()
        # transforms (key, value, key1, value1) to {key: value, key1, value1}
        # for both views & constraints
        return {
            "views": [
                dict(zip(dependency[::2], dependency[1::2]))
                for dependency in dependencies_view
                # dependency is a dict of {
                # dependant_schema: 'name_of_schema',
                # dependent_view : 'view_name',
                # source_schema: 'current_schema',
                # source_table: current_table_name
                # }
            ],
            "constraints": [
                dict(zip(dependency[::2], dependency[1::2]))
                for dependency in dependencies_fk
                # {'source_schema': schemaname,
                # 'dependent_schema_table': schema_table_name,
                # 'source_schema_table': source_schema_table
                # 'type_of_constraint': referencing_column::varchar,
                # 'dependent_schema' : schema_name
            ],
        }

    @staticmethod
    def get_dependant_tables_objects(database_connection: object, tables) -> [dict]:
        with database_connection.cursor() as cursor:
            cursor.execute(get_tables_view_dependencies(tables))
            dependencies_views = cursor.fetchall()
            cursor.execute(get_tables_fk_dependencies(tables))
            dependencies_fk = cursor.fetchall()
        return {
            "views": [
                dict(zip(dependency[::2], dependency[1::2]))
                for dependency in dependencies_views
            ],
            "constraints": [
                dict(zip(dependency[::2], dependency[1::2]))
                for dependency in dependencies_fk
            ],
        }

    @staticmethod
    def schema_exists(database_connection: object, schema_name: str) -> [bool]:
        """
        :param schema_name
        :param database_connection:
        :return: bool
        """
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{}');".format(
                    schema_name
                )
            )
            return cursor.fetchone()[0]

    @staticmethod
    def schema_table_exists(
        database_connection: object, schema_table_name: str
    ) -> [bool]:
        """
        :param table_name schema.table
        :param database_connection:
        :return: bool
        """
        schema_table_name = schema_table_name.split(".")
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = '{}' AND tablename = '{}');".format(
                    schema_table_name[0], schema_table_name[1]
                )
            )
            return cursor.fetchone()[0]

    @staticmethod
    def schema_view_exists(
        database_connection: object, schema_table_name: str
    ) -> [bool]:
        """
        :param schema_table_name: name of the view schema.viewname
        :param database_connection: connection instance of the database
        :return: bool
        """
        schema_table_name = schema_table_name.split(".")
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_views WHERE schemaname = '{}' AND viewname = '{}');".format(
                    schema_table_name[0], schema_table_name[1]
                )
            )
            return cursor.fetchone()[0]
