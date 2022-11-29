"""Console script for reims_publisher."""
import sys
import click
import questionary
from reims_publisher.core.database_manager import (
    get_services,
    get_conn_string_from_service_name,
)
from reims_publisher.core.information_schema import SchemaQuerier
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
    logger.path_to_log_file = 'depublication'
    logger.src_db = service_db_dst
    logger.dst_db = service_db_dst
    if object_type == SCHEMAS:
        process = main_schema_process(dst_conn, dst_conn, logger)
        confirm = questionary.confirm(
            "{} schéma(s), {} table(s) et {} vue(s) seront dépubliés".format(
                len(process["schemas"]), len(process["tables"]), len(process["views"])
            )
        ).ask()
        if confirm:
            depublish(
                dst_conn_string,
                logger.path_to_log_file,
                schemas=process["schemas"],
                force=True,
            )
    questionary.print("cmd_cli.py {}".format(logger.build_cmd_command()))
    logger.insert_log_row()
    questionary.print("Script de dépublication terminé")


def cli_publish():
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
    logger.path_to_log_file = 'publication'
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
        if logger.error_messages:
            questionary.print(
                "{} Erreurs rencontrées".format(logger.error_count_messages),
                style="bold italic fg:red",
            )
            questionary.print(logger.error_messages)
        force = True
        if not process["success"]:
            force = questionary.confirm(
                "Souhaitez-vous ignorer les erreurs et essayer de publier ?"
            ).ask()
        if not force:
            questionary.print(no_change_message())
            return
        logger = process["logger"]
        # Now publish
        confirm = questionary.confirm(
            "{} schéma(s), {} table(s) et {} vue(s) seront publiés".format(
                len(process["schemas"]), len(process["tables"]), len(process["views"])
            )
        ).ask()
        if confirm:
            publish(
                src_conn_string,
                dst_conn_string,
                logger.path_to_log_file,
                schemas=process["schemas"],
                force=force,
            )
        else:
            questionary.print("Script de publication annulé")

    elif object_type == TABLES:
        process = main_table_process(src_conn, dst_conn, logger)
        # Pre Process (dependencies, ect)
        logger = process["logger"]  # reattach logger
        if logger.error_messages:
            questionary.print(logger.error_message)
        force = False
        if not process["success"] and process["tables"] is not None:
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de publier ?"
            ).ask()
            if not force:
                questionary.print(no_change_message())
                return
        publish(
            src_conn_string,
            dst_conn_string,
            logger.path_to_log_file,
            tables=process["tables"],
            force=force,
        )

    logger.success = True
    questionary.print("cmd_cli.py {}".format(logger.build_cmd_command()))
    logger.insert_log_row()
    questionary.print("Script de publication terminé")
    src_conn.close()
    dst_conn.close()


def main_table_process(conn_src, conn_dst, logger):
    "Ask specific table questions"

    schema = questionary.select(
        "Selection du schéma", choices=SchemaQuerier.get_schemas(conn_src)
    ).ask()

    # Check if schemas exists, raise error if not
    if not SchemaQuerier.schema_exists(conn_dst, schema):
        logger.error_messages = no_schema_message(schema)
        return {"success": False, "tables": None, "logger": logger}

    tables = questionary.checkbox(
        "Selection du ou des tables",
        choices=SchemaQuerier.get_tables_from_schema(conn_src, schema),
        validate=choice_checker,
    ).ask()

    src_dependencies = SchemaQuerier.get_dependant_tables_objects(conn_src, tables)
    if src_dependencies["views"] is None and src_dependencies["constraints"] is None:
        questionary.print("Aucune dépendances")
        tables_dependencies = {"can_publish": True}  # TODO make it more amovran
    else:
        tables_dependencies = SchemaQuerier.can_publish_to_dst_server(
            conn_dst, src_dependencies, schemas=[schema], tables=tables
        )
    if not tables_dependencies["can_publish"]:
        logger.error_messages = (
            tables_dependencies["table_view_errors"]
            + tables_dependencies["schema_errors"]
        )
        return {"success": False, "tables": tables, "logger": logger}
    return {"success": True, "tables": tables, "logger": logger}


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
    if src_dependant["views"] is None and src_dependant["constraints"] is None:
        questionary.print("Aucune dépendances")
        schemas_dependencies = {
            "can_publish": True
        }  # TODO make it more amorvan approved
    else:
        schemas_dependencies = SchemaQuerier.can_publish_to_dst_server(
            conn_dst,
            src_dependant,
            schemas=logger.object_names,
            tables=tables_to_be_published,
        )

    if not schemas_dependencies["can_publish"]:
        logger.error_messages = (
            schemas_dependencies["table_view_errors"]
            + schemas_dependencies["schema_errors"]
        )
        success = False
    return {
        "success": success,
        "schemas": schemas,
        "tables": tables_to_be_published,
        "views": views_to_be_published,
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


def no_table_message(table_name):
    return (
        "La table {} ne se trouve pas "
        "sur le serveur de destination, merci de le créer/publier \n ".format(
            table_name
        )
    )


def no_change_message():
    return "Script de publication terminé sans avoir apporté de changement"


@click.command()
def main():
    publish_ = questionary.select(
        "Que souhaitez vous faire ?", choices=["Publier", "Dépuplier"]
    ).ask()
    if publish_ == "Publier":
        cli_publish()
    else:
        cli_depublish()


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
