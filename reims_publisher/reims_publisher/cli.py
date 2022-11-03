"""Console script for reims_publisher."""
import sys
import click
import questionary
from reims_publisher.core.database_manager import (
    get_services,
    get_conn_from_service_name,
)
from reims_publisher.core.information_schema import SchemaQuerier
from reims_publisher.core.publish import publish
from reims_publisher.core.logger import PublisherLogger

BASIC_POSTGRES_OBJECTS = {
    "Schemas": "schemas",
    "Tables": "tables",
    "Vues": "vues",
    "Vues Matérialisées": "materialized_views",
}


@click.command()
def main():
    available_services = get_services()
    service_db_src = questionary.select(
        "Selection de la base de données source", choices=available_services
    ).ask()

    # src_conn
    conn_src = get_conn_from_service_name(service_db_src)["conn"]
    src_conn_string = get_conn_from_service_name(service_db_src)["conn_str"]

    service_db_dst = questionary.select(
        "Selection de la base de données de destination",
        choices=list(filter(lambda x: x is not service_db_src, available_services)),
    ).ask()

    # dst_conn
    conn_dst = get_conn_from_service_name(service_db_dst)["conn"]
    dst_conn_string = get_conn_from_service_name(service_db_dst)["conn_str"]

    # init logger
    logger = PublisherLogger(conn_src)
    logger.src_db = service_db_src
    logger.dst_db = service_db_dst

    # What object
    object_ = questionary.select(
        "Que voulez-vous publier ?", choices=list(BASIC_POSTGRES_OBJECTS.keys())
    ).ask()
    logger.object_type = object_
    object_ = BASIC_POSTGRES_OBJECTS.get(object_)  # pretty

    if object_ == "schemas":
        process = main_schema_process(conn_src, conn_dst, logger)
        logger = process["logger"]
        # Pre Process (dependencies, ect)
        if logger.error_messages:
            questionary.print(
                "{} Erreurs rencontrées".format(len(logger.error_messages)),
                style="bold italic fg:red",
            )
            [questionary.print(error) for error in logger.error_messages]
        force = True
        if not process["success"]:
            force = questionary.confirm(
                "Souhaitez-vous ignorer les erreurs et essayer de publier ?"
            ).ask()
        if not force:
            logger.insert_log_row()
            questionary.print(no_change_message())
            return
        logger = process["logger"]
        # Now publish
        confirm = questionary.confirm(
            "{} schéma(s), {} tables et {} vues seront publiés".format(
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
            logger.error_messages("Annulé par l'utilisateur")
            logger.insert_log_row()
            questionary.print("Script de publication annulé")

    elif object_ == "tables":
        process = main_table_process(conn_src, conn_dst, logger)
        # Pre Process (dependencies, ect)
        logger = process["logger"]  # reattach logger
        if logger.errorerror_messages:
            questionary.print(logger.error_message)
        force = False
        if not process["success"] and process["tables"] is not None:
            force = questionary.confirm(
                "Souhaitez-vous ignorer les warnings et essayer de publier ?"
            ).ask()
            if not force:
                logger.insert_log_row()
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
    # get src dependencies
    src_dependencies = SchemaQuerier.get_dependant_schemas_objects(conn_src, schemas)
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
    if src_dependencies["views"] is None and src_dependencies["constraints"] is None:
        questionary.print("Aucune dépendances")
        schemas_dependencies = {
            "can_publish": True
        }  # TODO make it more amorvan approved
    else:
        schemas_dependencies = SchemaQuerier.can_publish_to_dst_server(
            conn_dst,
            src_dependencies,
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


def mystyle(a, b):
    import pdb

    pdb.set_trace()


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
