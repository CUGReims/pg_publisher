from typing import List, Optional

from reims_publisher.core.sql_queries import (
    get_schemas_dependencies,
    get_tables_view_dependencies,
    get_tables_fk_dependencies,
    get_schemas_fk_dependencies,
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
                " table_schema = '{schema}' AND"
                " table_type = 'BASE TABLE';".format(schema=schema_name)
            )
            tables = [
                "{schema}.{table}".format(schema=schema_name, table=table[0])
                for table in cursor.fetchall()
            ]
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
            dependencies_view = cursor.fetchall()
            cursor.execute(get_schemas_fk_dependencies(schemas))
            dependencies_fk = cursor.fetchall()
        return {
            "views": [
                dict(zip(dependency[::2], dependency[1::2]))
                for dependency in dependencies_view
            ],
            "constraints": [
                dict(zip(dependency[::2], dependency[1::2]))
                for dependency in dependencies_fk
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
    def can_publish_to_dst_server(
        database_connection: object,
        src_dependencies: [dict],
        schemas: Optional[List[str]] = None,
        tables: Optional[List[str]] = None,
    ) -> {dict}:
        """
        Checks for each src dependencies, that dst database has the correct dependencies.
        Will check that dependent Schemas, Tables and FK exists.
        :param schemas: list of schemas specified by the user
        :param tables: list of tables either in schemas or specified by the user
        :param database_connection: the connection to the dst database
        :param src_dependencies: list of dependencies from schemas or tables
        :return: {can_publish : Bool : schema_errors: [], schema_errors: []}
        """
        schema_errors = []
        table_view_errors = []
        # Keep only schemas that aren't referenced in current publish task
        get_unique_source_schemas = list(
            set(
                val["source_schema"]
                for val in src_dependencies["constraints"] + src_dependencies["views"]
            )
        )
        schemas_not_specified = [
            schema for schema in get_unique_source_schemas if schema not in schemas
        ]
        # Add error if schema not in current publish task nor in dst server
        for schema in schemas_not_specified:
            if not SchemaQuerier.schema_exists(database_connection, schema):
                schema_errors.insert(0, no_schema_message(schema))

        # Keep only tables that aren't referenced in current publish task
        get_unique_source_tables = list(
            set(
                val["schema_table"]
                for val in src_dependencies["constraints"] + src_dependencies["views"]
            )
        )
        # Add error if table not in current publish task nor in dst server
        tables_not_specified = [
            table for table in get_unique_source_tables if table not in tables
        ]

        for table in tables_not_specified:
            if not SchemaQuerier.schema_table_exists(database_connection, table):
                schema_errors.insert(0, no_table_message(table))

        # Check each dependencies to make sure it can be published
        for dep in src_dependencies["constraints"]:
            schema_table_name = dep["source_schema_table"]
            dependent_schema_table_name = dep["schema_table"]
            if (
                dependent_schema_table_name in tables_not_specified
                and not SchemaQuerier.schema_table_exists(
                    database_connection, schema_table_name
                )
            ):
                # check that table exists in dest
                table_view_errors.insert(
                    0,
                    "La contrainte {} de la table {} fait référence à la table {} qui existe pas".format(
                        dep["type_of_constraint"],
                        dep["source_schema_table"],
                        dep["schema_table"],
                    ),
                )
        for dep in src_dependencies["views"]:
            schema_table_name = dep["schema_table"]
            dependent_schema_table_name = "{}.{}".format(
                dep["dependent_schema"], dep["dependent_table"]
            )
            if (
                dependent_schema_table_name in tables_not_specified
                and not SchemaQuerier.schema_table_exists(
                    database_connection, schema_table_name
                )
            ):
                table_view_errors.insert(
                    0,
                    "La vue {} du schéma {} depend de la table {} du schéma {}".format(
                        dep["view"],
                        dep["source_schema"],
                        dep["dependent_table"],
                        dep["dependent_schema"],
                    ),
                )
        return {
            "can_publish": True
            if len(schema_errors) == 0 and len(table_view_errors) == 0
            else False,
            "schema_errors": schema_errors,
            "table_view_errors": table_view_errors,
        }

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


def no_schema_message(schema_name):
    return (
        "Le schema {} ne se trouve pas "
        "sur le serveur de destination, merci de le créer/publier \n ".format(
            schema_name
        )
    )


def no_table_message(table_name):
    return (
        "La table {} ne se trouve pas "
        "sur le serveur de destination, merci de le créer/publier \n ".format(
            table_name
        )
    )
