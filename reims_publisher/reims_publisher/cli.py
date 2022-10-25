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
    "Vues": "materialized_views",
    "View Matéralisées": "materialized_views",
}


@click.command()
def main():
    available_services = get_services()
    service_db_src = questionary.select(
        "Selection de la base de données source", choices=available_services
    ).ask()

    conn_src = get_conn_from_service_name(service_db_src)["conn"]
    src_conn_string = get_conn_from_service_name(service_db_src)["conn_str"]

    # remove selected service
    available_services = list(
        filter(lambda x: x is not service_db_src, available_services)
    )
    service_db_dst = questionary.select(
        "Selection de la base de données de destination", choices=available_services
    ).ask()

    conn_dst = get_conn_from_service_name(service_db_dst)["conn"]
    dst_conn_string = get_conn_from_service_name(service_db_dst)["conn_str"]

    logger = PublisherLogger(conn_src)
    logger.success = False
    logger.src_db = service_db_src
    logger.src_db = service_db_dst

    # What object
    object_ = questionary.select(
        "Que voulez-vous publier ?", choices=list(BASIC_POSTGRES_OBJECTS.keys())
    ).ask()
    logger.object_type = object_

    object_ = BASIC_POSTGRES_OBJECTS.get(object_)
    if object_ == "schemas":
        process = schema_choice(conn_src, conn_dst, logger)
        if not process["success"]:
            questionary.print(
                "Script de publication terminé sans avoir apporté de changement"
            )
            return
        logger = process["logger"]
        publish(
            src_conn_string,
            dst_conn_string,
            logger.path_to_log_file,
            schemas=process["schemas"],
            force=process["force"],
        )
    elif object_ == "tables":
        process = table_choice(conn_src, conn_dst, logger)
        if not process["success"]:
            return
        logger = process["logger"]
        publish(
            src_conn_string,
            dst_conn_string,
            logger.path_to_log_file,
            tables=process["tables"],
            force=process["force"],
        )
    logger.success = True
    logger.insert_log_row()
    questionary.print("Script de publication terminé")


def table_choice(conn_src, conn_dst, logger):
    force = True
    schema = questionary.select(
        "Selection du schéma", choices=SchemaQuerier.get_schemas(conn_src)
    ).ask()
    # verification
    if not SchemaQuerier.schema_exists(conn_dst, schema):
        error_message = "Le schéma {schema} ne se trouve pas sur le serveur de destination".format(
            schema=schema
        )
        questionary.print(
            "{error_message}, merci de le créer et de relancer".format(
                error_message=error_message, schema=schema
            )
        )
        logger.success = False
        logger.fail_reason = error_message
        logger.insert_log_row()
        return {"success": False, "logger": logger}

    tables = questionary.checkbox(
        "Selection du ou des tables",
        choices=SchemaQuerier.get_tables_from_schema(conn_src, schema),
        validate=choice_checker,
    ).ask()

    logger.object_names = tables
    dependencies = SchemaQuerier.get_dependant_tables_objects(conn_dst, tables)
    if dependencies["views"] or dependencies["tables"]:
        questionary.print(
            "Des dépendances ont été trouvé pour la/les table(s) {}".format(
                ",".join(tables)
            )
        )
        return {
            **table_dependencies_manager(dependencies, logger),
            **{"tables": tables, "force": force},
        }

    return {"success": True, "tables": tables, "force": force, "logger": logger}


def schema_choice(conn_src, conn_dst, logger) -> dict:
    force = True
    schemas = questionary.checkbox(
        "Selection du ou des schémas",
        choices=SchemaQuerier.get_schemas(conn_src),
        validate=choice_checker,
    ).ask()
    logger.object_names = schemas
    dependencies = SchemaQuerier.get_dependant_schemas_objects(conn_src, schemas)
    if dependencies:
        questionary.print("Des dépendences ont été trouvé:")
        return {
            **schema_dependencies_manager(dependencies, conn_dst, logger),
            **{"schemas": schemas, "force": force},
        }
    return {"success": True, "schemas": schemas, "force": force, "logger": logger}


def table_dependencies_manager(dependencies, logger):
    warning_text = "[INFO]: "
    for dep in dependencies["views"]:
        warning_text += "La vue {} du schéma {} depend de la table {} du schéma {} \n".format(
            dep["dependent_view"],
            dep["dependent_schema"],
            dep["source_schema"],
            dep["source_table"],
        )
    for dep in dependencies["tables"]:
        warning_text += "La table {} depend de la table {} avec la contrainte {}".format(
            dep["dependent_table"], dep["schema_table"], dep["type_of_constraint"]
        )
    questionary.print("Des dépendances ont été trouvé \n {}".format(warning_text))
    force = questionary.confirm("Voulez vous forcer la suppresion?").ask()
    if not force:
        logger.fail_reason = "Opération annulée"
        return {"success": False, "logger": logger}
    return {"success": True, "logger": logger}


def schema_dependencies_manager(dependencies, conn_dst, logger):
    error_message = None
    warning_text = None
    for dep in dependencies:
        if not SchemaQuerier.schema_exists(conn_dst, dep["source_schema"]):
            # Cela indique que le schema courant de pourra pas être publier
            error_message = (
                "[ERREUR]: Le schema {} ne se trouve pas "
                "sur le serveur de destination, merci de le créer \n ".format(
                    dep["source_schema"]
                )
            )
        # TODO DEMANDE A PIERRE C'est pour information.
        warning_text = "[INFO]: La vue {} du schéma {} depend de la table {} du schéma {}\n".format(
            dep["dependent_view"],
            dep["dependent_schema"],
            dep["source_table"],
            dep["source_schema"],
        )
    if warning_text:
        questionary.print(warning_text)
    if error_message:
        questionary.print(error_message)
        return {"success": False, "logger": logger}
        # list all dependencies
    return {"success": True, "logger": logger}


def choice_checker(e):
    return "Au moins un élément doit être sélectionné " if len(e) == 0 else True


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
