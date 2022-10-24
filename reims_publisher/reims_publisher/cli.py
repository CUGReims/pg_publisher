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
def main(args=None):
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

    object = questionary.select(
        "Que voulez-vous publier ?", choices=list(BASIC_POSTGRES_OBJECTS.keys())
    ).ask()
    logger.object_type = object

    object = BASIC_POSTGRES_OBJECTS.get(object)
    if object == "schemas":
        force = True
        schemas = questionary.checkbox(
            "Selection du ou des schémas",
            choices=SchemaQuerier.get_schemas(conn_src),
            validate=choice_checker,
        ).ask()
        logger.object_names = schemas
        dependencies = SchemaQuerier.get_dependant_schemas_objects(conn_dst, schemas)
        if dependencies:
            questionary.print(
                "Des dépendances ont été trouvé pour le(s) schéma(s) {}".format(
                    ",".join(schemas)
                )
            )
            questionary.print(dependencies)
            force = questionary.confirm("Voulez vous forcer la suppresion?").ask()

        publish(src_conn_string, dst_conn_string, schemas=schemas, force=force)
        logger.success = True
        logger.insert_log_row()


def choice_checker(e):
    return "Au moins un élément doit être sélectionné " if len(e) == 0 else True


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
