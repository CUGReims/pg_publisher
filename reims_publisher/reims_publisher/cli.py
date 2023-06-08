"""Console script for reims_publisher."""
import sys
import click
import questionary
from reims_publisher.core.database_manager import (
    get_services,
    get_conn_string_from_service_name,
)
from reims_publisher.core.information_schema import SchemaQuerier
from reims_publisher.core.publish_checker import can_publish_to_dst_server
from reims_publisher.core.publish import publish
from reims_publisher.core.depublish import depublish
from reims_publisher.core.logger import PublisherLogger
from psycopg2 import connect

SCHEMAS = "schemas"
TABLES = "tables"
VIEWS = "views"
MAT_VIEW = "materialized_views"

BASIC_POSTGRES_OBJECTS = {
    SCHEMAS: "Schemas",
    TABLES: "Tables",
    VIEWS: "Vues",
    MAT_VIEW: "Vues Matérialisées",
}


def cli_depublish():

    available_services = get_services()
    service_db_dst = questionary.select(
        "Selection de la base de données", choices=available_services
    ).ask()

    # dst_conn

    dst_conn_string = get_conn_string_from_service_name(service_db_dst)
    dst_conn = connect(dst_conn_string)

    # What object
    object_type = questionary.select(
        "Que voulez-vous dépublier ?", choices=list(BASIC_POSTGRES_OBJECTS.keys())
    ).ask()
    # init logger
    logger = PublisherLogger(dst_conn)
    logger.publish_type = "depublication"
    logger.src_db = service_db_dst
    logger.dst_db = service_db_dst
    logger.object_type = object_type
    if object_type == SCHEMAS:
        process = main_schema_process(dst_conn, dst_conn, logger)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(",".join(logger.error_messages))
        force = True
        if not process["success"] and process["tables"] is not None:
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de depublier ?"
            ).ask()
        for dep in process["schema_dependencies_depublish"]:
            questionary.print(dep, style="bold italic fg:yellow")
        confirm = questionary.confirm(
            "{} schéma(s), {} table(s), {} vue(s) et {} vues matéralisée(s) seront "
            "dépubliés de la base {}".format(
                len(process["schemas"]),
                len(process["tables"]),
                len(process["views"]),
                len(process["materialized_views"]),
                service_db_dst,
            )
        ).ask()
        if confirm:
            depublish(
                dst_conn_string,
                logger.path_to_log_file,
                schemas=process["schemas"],
                force=force,
            )
            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de dépublication terminé")
        else:
            questionary.print("Script de dépublication annulé")
    if object_type == TABLES:
        force = True
        process = main_table_process(dst_conn, dst_conn, logger)
        # Pre Process (dependencies, ect)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(",".join(logger.error_messages))
        if not process["tables"]:
            questionary.print(no_change_message())
        # check for warnings
        if process["views_dep"]:
            questionary.print(
                ",\n".join(process["views_dep"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et continuer la dépublication ?"
            ).ask()
        confirm = questionary.confirm(
            "{} table(s) et {} vue(s) seront dépubliée(s) de la base {}".format(
                len(process["tables"]), len(process["views_dep"]), service_db_dst
            )
        ).ask()
        if confirm:
            depublish(
                dst_conn_string,
                logger.path_to_log_file,
                tables=process["tables"],
                force=force,
            )

            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de dépublication terminé")
        else:
            questionary.print("Script de dépublication annulé")
    if object_type == VIEWS:
        force = True
        process = main_view_process(dst_conn, dst_conn, logger)
        # Pre Process (dependencies, ect)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(",".join(logger.error_messages))
        if not process["views"]:
            questionary.print(no_change_message())
            return
        # check for warnings
        if process["views_dep"]:
            questionary.print(
                ",\n".join(process["views_dep"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et continuer la dépublication ?"
            ).ask()
        confirm = questionary.confirm(
            "{} vue(s) et {} vue(s) dépendantes seront dépubliées de la base {}".format(
                len(process["views"]), len(process["views_dep"]), service_db_dst
            )
        ).ask()
        if confirm:
            depublish(
                dst_conn_string,
                logger.path_to_log_file,
                views=process["views"],
                force=force,
            )

            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de dépublication terminé")
        else:
            questionary.print("Script de dépublication annulé")
    if object_type == MAT_VIEW:
        force = True
        process = main_mat_view_process(dst_conn, dst_conn, logger)

        # Pre Process (dependencies, ect)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(",".join(logger.error_messages))
        if not process["mat_views"]:
            questionary.print(no_change_message())
            return
        # check for warnings
        if process["views_dep"]:
            questionary.print(
                ",\n".join(process["views_dep"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et continuer la dépublication ?"
            ).ask()

        confirm = questionary.confirm(
            "{} vue(s) matérialisée(s) et {} vue(s) dépendantes seront dépubliées de la base {}".format(
                len(process["mat_views"]), len(process["views_dep"]), service_db_dst
            )
        ).ask()
        if confirm:
            depublish(
                dst_conn_string,
                logger.path_to_log_file,
                materialized_views=process["mat_views"],
                force=force,
            )

            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de dépublication terminé")
        else:
            questionary.print("Script de dépublication annulé")
    dst_conn.close()


def cli_publish(no_acl_no_owner):
    available_services = get_services()
    service_db_src = questionary.select(
        "Selection de la base de données source", choices=available_services
    ).ask()

    # src_conn
    src_conn_string = get_conn_string_from_service_name(service_db_src)
    src_conn = connect(src_conn_string)

    service_db_dst = questionary.select(
        "Selection de la base de données de destination",
        choices=list(filter(lambda x: x is not service_db_src, available_services)),
    ).ask()

    # dst_conn
    dst_conn_string = get_conn_string_from_service_name(service_db_dst)
    dst_conn = connect(dst_conn_string)

    # init logger
    logger = PublisherLogger(dst_conn)
    publish_type = "publication_with_acl_owner" if no_acl_no_owner else "publication"
    logger.publish_type = publish_type
    logger.src_db = service_db_src
    logger.dst_db = service_db_dst

    # What object
    object_type = questionary.select(
        "Que voulez-vous publier ?", choices=list(BASIC_POSTGRES_OBJECTS.keys())
    ).ask()
    logger.object_type = object_type
    if object_type == SCHEMAS:
        process = main_schema_process(src_conn, dst_conn, logger)
        logger = process["logger"]
        # Pre Process (dependencies, ect)
        if logger.error_count_messages > 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
        questionary.print(",".join(logger.error_messages))
        force = True
        if not process["success"]:
            force = questionary.confirm(
                "Souhaitez-vous ignorer les erreurs et essayer de publier ?"
            ).ask()
        if len(process["schema_warnings"]) > 0:
            questionary.print(
                ",".join(process["schema_warnings"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de publier ?"
            ).ask()
        if not force:
            questionary.print(no_change_message())
            return
        logger = process["logger"]
        # Now publish
        confirm = questionary.confirm(
            "{} schéma(s), {} table(s) et {} vue(s) seront publiés de la base {} vers la base {}".format(
                len(process["schemas"]),
                len(process["tables"]),
                len(process["views"]),
                service_db_src,
                service_db_dst,
            )
        ).ask()
        if confirm:
            publish(
                src_conn_string,
                dst_conn_string,
                logger.path_to_log_file,
                schemas=process["schemas"],
                no_acl_no_owner=no_acl_no_owner,
                force=force,
            )
            logger.success = True
            questionary.print(
                "python3 cli_direct.py {}".format(logger.build_cmd_command())
            )
            logger.insert_log_row()
            questionary.print("Script de publication terminé")
        else:
            questionary.print("Script de publication annulé")

    elif object_type == TABLES:
        process = main_table_process(src_conn, dst_conn, logger)
        # Pre Process (dependencies, ect)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(",".join(logger.error_messages))

        force = True
        # check for warnings
        if process["views_dep"]:
            questionary.print(
                ",\n".join(process["views_dep"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de publier ?"
            ).ask()
        if not force or not process["tables"]:
            questionary.print(no_change_message())
            return
        logger = process["logger"]
        # Now publish
        confirm = questionary.confirm(
            "{} table(s) seront publiés de la base {} vers la base {}".format(
                len(process["tables"]), service_db_src, service_db_dst
            )
        ).ask()
        if confirm:
            publish(
                src_conn_string,
                dst_conn_string,
                logger.path_to_log_file,
                tables=process["tables"],
                no_acl_no_owner=no_acl_no_owner,
                force=force,
            )
            logger.success = True
            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de publication terminé")
        else:
            questionary.print("Script de publication annulé")

    elif object_type == VIEWS:
        process = main_view_process(src_conn, dst_conn, logger)
        # Pre Process (dependencies, ect)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(",".join(logger.error_messages))

        force = True
        # check for warnings
        if process["views_dep"]:
            questionary.print(
                ",\n".join(process["views_dep"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de publier ?"
            ).ask()
        if not force or not process["views"]:
            questionary.print(no_change_message())
            return
        logger = process["logger"]
        # Now publish
        confirm = questionary.confirm(
            "{} vues(s) seront publiée(s) de la base {} vers la base {}".format(
                len(process["views"]), service_db_src, service_db_dst
            )
        ).ask()
        publish(
            src_conn_string,
            dst_conn_string,
            logger.path_to_log_file,
            views=process["views"],
            force=force,
            no_acl_no_owner=no_acl_no_owner,
        )
        if confirm:
            logger.success = True
            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de publication terminé")
        else:
            questionary.print("Script de publication annulé")
    elif object_type == MAT_VIEW:
        process = main_mat_view_process(src_conn, dst_conn, logger)
        # Pre Process (dependencies, ect)
        if logger.error_count_messages != 0:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            # TODO HERE
            # DROP CASCADE
            # DROP Dependences des schemas
            # de pas afficher les schémas ne contenant pas les vues si c'est vues selectionner
            # idem pour les tables
            questionary.print(",".join(logger.error_messages))

        force = True
        # check for warnings
        if process["views_dep"]:
            questionary.print(
                ",\n".join(process["views_dep"]), style="bold italic fg:yellow"
            )
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de publier ?"
            ).ask()
        if not force or not process["mat_views"]:
            questionary.print(no_change_message())
            return
        logger = process["logger"]
        # Now publish
        confirm = questionary.confirm(
            "{} vues(s) matérialisée(s) seront publiée(s) de la base {} vers la base {}".format(
                len(process["mat_views"]), service_db_src, service_db_dst
            )
        ).ask()
        if confirm:
            publish(
                src_conn_string,
                dst_conn_string,
                logger.path_to_log_file,
                materialized_views=process["mat_views"],
                no_acl_no_owner=no_acl_no_owner,
                force=force,
            )
            logger.success = True
            questionary.print("cli_direct.py {}".format(logger.build_cmd_command()))
            logger.insert_log_row()
            questionary.print("Script de publication terminé")
        else:
            questionary.print("Script de publication annulé")

    src_conn.close()
    dst_conn.close()


def main_table_process(conn_src, conn_dst, logger):
    "Ask specific table questions"

    schema = questionary.select(
        "Selection du schéma", choices=SchemaQuerier.get_schemas(conn_src)
    ).ask()

    # Check if schemas exists, raise error if not
    if not SchemaQuerier.schema_exists(conn_dst, schema):
        logger.error_messages.append(no_schema_message(schema))
        return {"success": False, "tables": [], "logger": logger, "views_dep": None}

    existing_tables = SchemaQuerier.get_tables_from_schema(conn_src, schema)
    if not existing_tables:
        logger.error_messages.append(no_table_in_schema(schema))
        return {"success": False, "tables": [], "logger": logger, "views_dep": None}
    tables = questionary.checkbox(
        "Selection du ou des tables", choices=existing_tables, validate=choice_checker
    ).ask()

    src_dependencies = SchemaQuerier.get_dependant_tables_objects(conn_src, tables)
    if src_dependencies["views"] is None and src_dependencies["constraints"] is None:
        questionary.print("Aucune dépendances")
        tables_dependencies = {"can_publish": True}  # TODO make it more amovran
    else:
        tables_dependencies = can_publish_to_dst_server(
            conn_dst, src_dependencies, schemas=[schema], tables=tables
        )
    logger.object_names = tables
    if not tables_dependencies["can_publish"]:
        logger.error_messages.append(tables_dependencies["table_view_errors"])
        logger.error_messages.append(tables_dependencies["schema_errors"])

        return {
            "success": False,
            "tables": tables,
            "views_dep": tables_dependencies["table_view_warnings"],
            "logger": logger,
        }
    return {
        "success": True,
        "tables": tables,
        "views_dep": tables_dependencies["table_view_warnings"],
        "logger": logger,
    }


def main_view_process(conn_src, conn_dst, logger):
    "Ask specific view questions"

    schema = questionary.select(
        "Selection du schéma", choices=SchemaQuerier.get_schemas_with_views(conn_src)
    ).ask()

    # Check if schemas exists, raise error if not
    if not SchemaQuerier.schema_exists(conn_dst, schema):
        logger.error_messages.append(no_schema_message(schema))
        return {"success": False, "views": [], "logger": logger, "views_dep": None}

    existing_views = SchemaQuerier.get_views_from_schema(conn_src, schema)
    if not existing_views:
        logger.error_messages.append(no_view_in_schema(schema))
        return {"success": False, "views": [], "logger": logger, "views_dep": None}
    views = questionary.checkbox(
        "Selection du ou des vues", choices=existing_views, validate=choice_checker
    ).ask()
    logger.object_names = views
    src_dependencies = SchemaQuerier.get_dependant_tables_objects(conn_src, views)

    if src_dependencies["views"] is None and src_dependencies["constraints"] is None:
        questionary.print("Aucune dépendances")
        tables_dependencies = {"can_publish": True}  # TODO make it more amovran
    else:
        tables_dependencies = can_publish_to_dst_server(
            conn_dst, src_dependencies, schemas=[schema], views=views
        )
    if not tables_dependencies["can_publish"]:
        logger.error_messages.append(tables_dependencies["table_view_errors"])
        logger.error_messages.append(tables_dependencies["schema_errors"])

        return {
            "success": False,
            "views": views,
            "views_dep": tables_dependencies["table_view_warnings"],
            "logger": logger,
        }
    return {
        "success": True,
        "views": views,
        "views_dep": tables_dependencies["table_view_warnings"],
        "logger": logger,
    }


def main_mat_view_process(conn_src, conn_dst, logger):
    "Ask specific mat view questions"

    schema = questionary.select(
        "Selection du schéma", choices=SchemaQuerier.get_schemas_with_matviews(conn_src)
    ).ask()

    # Check if schemas exists, raise error if not
    if not SchemaQuerier.schema_exists(conn_dst, schema):
        logger.error_messages.append(no_schema_message(schema))
        return {"success": False, "mat_views": [], "logger": logger, "views_dep": None}

    existing_mat_views = SchemaQuerier.get_materialized_views_from_schema(
        conn_src, schema
    )
    if not existing_mat_views:
        logger.error_messages.append(no_mat_view_in_schema(schema))
        return {"success": False, "mat_views": [], "logger": logger, "views_dep": None}
    mat_views = questionary.checkbox(
        "Selection du ou des vues matérialisées",
        choices=existing_mat_views,
        validate=choice_checker,
    ).ask()

    src_dependencies = SchemaQuerier.get_dependant_tables_objects(
        conn_src, existing_mat_views
    )

    if src_dependencies["views"] is None and src_dependencies["constraints"] is None:
        questionary.print("Aucune dépendances")
        tables_dependencies = {"can_publish": True}  # TODO make it more amovran
    else:
        tables_dependencies = can_publish_to_dst_server(
            conn_dst, src_dependencies, schemas=[schema], materialized_views=mat_views
        )
    logger.object_names = mat_views
    if not tables_dependencies["can_publish"]:
        logger.error_messages.append(tables_dependencies["table_view_errors"])
        logger.error_messages.append(tables_dependencies["schema_errors"])
        logger.error_messages.append(tables_dependencies["schema_errors"])

        return {
            "success": False,
            "mat_views": mat_views,
            "views_dep": tables_dependencies["table_view_warnings"],
            "logger": logger,
        }
    return {
        "success": True,
        "mat_views": mat_views,
        "views_dep": tables_dependencies["table_view_warnings"],
        "logger": logger,
    }


def main_schema_process(conn_src, conn_dst, logger) -> dict:
    """Ask specific schema questions"""
    success = True
    schemas = questionary.checkbox(
        "Selection du ou des schémas",
        choices=SchemaQuerier.get_schemas(conn_src),
        validate=choice_checker,
    ).ask()
    logger.object_names = schemas
    # get src dependant
    src_dependant = SchemaQuerier.get_dependant_schemas_objects(conn_src, schemas)

    questionary.print("Vérifications des dépendances...")
    tables_to_be_published = list(
        set(
            [
                item
                for sublist in [
                    SchemaQuerier.get_tables_from_schema(conn_src, schema)
                    for schema in schemas
                ]
                for item in sublist
            ]
        )
    )
    views_to_be_published = list(
        set(
            [
                item
                for sublist in [
                    SchemaQuerier.get_views_from_schema(conn_src, schema)
                    for schema in schemas
                ]
                for item in sublist
            ]
        )
    )
    materialized_views_to_be_published = list(
        set(
            [
                item
                for sublist in [
                    SchemaQuerier.get_materialized_views_from_schema(conn_src, schema)
                    for schema in schemas
                ]
                for item in sublist
            ]
        )
    )
    if (
        src_dependant["views"] is None
        and src_dependant["constraints"] is None
        and src_dependant["dependencies"] is None
    ):
        questionary.print("Aucune dépendances")
        schemas_dependencies = {
            "can_publish": True
        }  # TODO make it more amorvan approved
    else:
        schemas_dependencies = can_publish_to_dst_server(
            conn_dst, src_dependant, schemas=schemas, tables=tables_to_be_published
        )
    if not schemas_dependencies["can_publish"]:
        for schema_dep_error in (
            schemas_dependencies["table_view_errors"]
            + schemas_dependencies["schema_errors"]
        ):
            logger.error_messages.append(schema_dep_error)

        success = False
    return {
        "success": success,
        "schemas": schemas,
        "tables": tables_to_be_published,
        "views": views_to_be_published,
        "schema_dependencies_depublish": schemas_dependencies[
            "schema_dependencies_depublish"
        ],
        "schema_warnings": schemas_dependencies["schema_warnings"],
        "materialized_views": materialized_views_to_be_published,
        "logger": logger,
    }


def choice_checker(e):
    return "Au moins un élément doit être sélectionné " if len(e) == 0 else True


def no_schema_message(schema_name):
    return (
        "Le schema {} ne se trouve pas "
        "sur le serveur de destination, merci de le créer/publier \n ".format(
            schema_name
        )
    )


def no_table_in_schema(schema_name):
    return "Aucune table se trouve dans le schema {}".format(schema_name)


def no_view_in_schema(schema_name):
    return "Aucune vue se trouve dans le schema {}".format(schema_name)


def no_mat_view_in_schema(schema_name):
    return "Aucune vue matérialisée se trouve dans le schema {}".format(schema_name)


def no_change_message():
    return "Script de publication terminé sans avoir apporté de changement"


@click.command()
def main():
    response = questionary.select(
        "Que souhaitez vous faire ?",
        choices=["Publier", "Publier avec les droits", "Dépuplier"],
    ).ask()
    if response == "Publier":
        cli_publish(True)
    elif response == "Publier avec les droits":
        cli_publish(False)
    elif response == "Dépuplier":
        cli_depublish()
    elif response is None:
        return


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
