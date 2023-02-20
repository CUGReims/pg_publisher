from typing import List, Optional
from reims_publisher.core.information_schema import SchemaQuerier


def can_publish_to_dst_server(
    database_connection: object,
    src_dependencies: [dict],
    schemas: Optional[List[str]] = [],
    tables: Optional[List[str]] = [],
    views: Optional[List[str]] = [],
    materialized_views: Optional[List[str]] = [],
) -> {dict}:
    """
    Checks that all dependencies of current objects exist in dst database.
    :param schemas: list of schemas specified by the user
    :param tables: list of tables either in schemas or specified by the user
    :param views: list of views either in schemas or specified by the user
    :param materialized_views: list of materialized views in schemas or specified by the user
    :param database_connection: the connection to the dst database
    :param src_dependencies: list of dependencies from schemas or tables
    :return: {can_publish : Bool : schema_errors: [publish_type], table_view_errors: []}
    """
    schema_errors = []
    schema_dependencies_depublish = []
    table_view_errors = []
    table_view_warnings = []
    # Keep only schemas that aren't referenced in current publish task
    get_unique_source_schemas = list(
        set(
            val["source_schema"]
            for val in src_dependencies["constraints"] + src_dependencies["views"]
        )
    )
    if schemas:
        for dep in SchemaQuerier.get_all_tables_views_in_schema(
            database_connection, schemas
        ):
            schema_dependencies_depublish.append(schema_dependence_message(dep))

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
            val["dependent_schema_table"]
            for val in src_dependencies["constraints"]
            + src_dependencies["views"]
            + src_dependencies["dependencies"]
        )
    )
    tables_not_specified = [
        table for table in get_unique_source_tables if table not in tables
    ]
    for table in tables_not_specified:
        if not SchemaQuerier.schema_table_exists(database_connection, table):
            schema_errors.insert(0, no_table_message(table))
        else:
            schema_errors.insert(0, has_reference_message(table))

    if len(views) != 0:
        get_unique_source_views = list(
            set(
                val["dependent_schema_table"]
                for val in src_dependencies["constraints"] + src_dependencies["views"]
            )
        )
        # Add error if table not in current publish task nor in dst server
        views_not_specified = [
            view for view in get_unique_source_views if view not in views
        ]
        for view in views_not_specified:
            if not SchemaQuerier.schema_view_exists(database_connection, view):
                schema_errors.insert(0, no_view_message(view))
    if len(materialized_views) != 0:
        get_unique_source_mat_views = list(
            set(
                val["dependent_schema_table"]
                for val in src_dependencies["constraints"] + src_dependencies["views"]
            )
        )
        # Add error if table not in current publish task nor in dst server
        mat_views_not_specified = [
            mat_view
            for mat_view in get_unique_source_mat_views
            if mat_view not in materialized_views
        ]
        for mat_view in mat_views_not_specified:
            if not SchemaQuerier.schema_view_exists(database_connection, mat_view):
                schema_errors.insert(0, no_mat_view_message(mat_view))

    # Check each dependencies to make sure it can be published
    for dep in src_dependencies["constraints"]:
        schema_table_name = dep["source_schema_table"]
        dependent_schema_table_name = dep["dependent_schema_table"]
        if (
            dependent_schema_table_name in tables_not_specified
            and not SchemaQuerier.schema_table_exists(
                database_connection, dependent_schema_table_name
            )
        ):
            # check that table exists in dest
            table_view_errors.insert(
                0,
                "La contrainte {} de la table {} fait référence à la table {} qui existe pas".format(
                    dep["type_of_constraint"],
                    schema_table_name,
                    dependent_schema_table_name,
                ),
            )
    for dep in src_dependencies["views"]:
        schema_table_name = dep["dependent_schema_table"]
        dependent_schema_table_name = "{}.{}".format(
            dep["dependent_schema"], dep["dependent_table"]
        )
        # When publishing check that all dep exists
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
        # when republishing, add warning to user that dependencies will be lost
        else:
            table_view_warnings.insert(
                0,
                "La vue {} du schéma {} dependant de la table {} du schéma {} sera supprimé. ".format(
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
        "schema_errors": list(set(schema_errors)),
        "schema_dependencies_depublish": schema_dependencies_depublish,
        "table_view_errors": list(set(table_view_errors)),
        "table_view_warnings": list(set(table_view_warnings)),
    }


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


def no_view_message(view_name):
    return (
        "La vue {} ne se trouve pas "
        "sur le serveur de destination, merci de le créer/publier \n ".format(view_name)
    )


def no_mat_view_message(mat_view_name):
    return (
        "La vue matérialisée {} ne se trouve pas "
        "sur le serveur de destination, merci de le créer/publier \n ".format(
            mat_view_name
        )
    )


def schema_dependence_message(dep_tuple):
    return "La {} {} se trouve dans le schema en cours de dépublication et sera supprimée".format(
        dep_tuple[0], dep_tuple[1]
    )


def has_reference_message(table_name):
    return (
        "La table {} utilise un objet du schéma courant, "
        "cette référence ou cette table sera supprimée\n ".format(table_name)
    )
