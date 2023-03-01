import argparse
from reims_publisher.core.publish import publish
from reims_publisher.core.depublish import depublish
from reims_publisher.core.logger import PublisherLogger
from reims_publisher.core.database_manager import get_conn_string_from_service_name
from psycopg2 import connect

# Initialize parser
parser = argparse.ArgumentParser()


parser.add_argument(
    "-src_db",
    "--SourceDatabaseServiceName",
    required=True,
    help="service value source database from PGSERVICEFILE",
)
parser.add_argument(
    "-dst_db",
    "--DestinationDatabaseServiceName",
    required=True,
    help="service value for destination database from PGSERVICEFILE",
)
parser.add_argument(
    "-ty",
    "--Type",
    required=True,
    help="publication, publication_with_acl_owner or depublication task",
)
parser.add_argument("-s", "--Schemas", help="List schemas with ; separator")
parser.add_argument("-t", "--Tables", help="List tables with ; separator")
parser.add_argument("-v", "--Views", help="List views with ; separator")
parser.add_argument(
    "-mv", "--MatViews", help="List materialized views with ; separator"
)

# Read arguments from command line
args = parser.parse_args()
print(args.SourceDatabaseServiceName)
print(args.DestinationDatabaseServiceName)
print(args.Type)

# get conn
service_db_dst = args.DestinationDatabaseServiceName
service_db_src = args.SourceDatabaseServiceName
src_conn_string = get_conn_string_from_service_name(service_db_src)
dst_conn_string = get_conn_string_from_service_name(service_db_dst)
dst_conn = connect(
    get_conn_string_from_service_name(args.DestinationDatabaseServiceName)
)

logger = PublisherLogger(dst_conn)
logger.src_db = service_db_dst
logger.dst_db = service_db_dst
schemas = tables = views = mat_views = []

if args.Schemas:
    schemas = args.Schemas.split(",")
    logger.object_names = schemas
    logger.object_type = "schemas"
elif args.Tables:
    tables = args.Tables
    logger.object_names = tables.split(",")
    logger.object_type = "tables"
elif args.Views:
    views = args.Views
    logger.object_names = views.split(",")
    logger.object_type = "views"
else:
    mat_views = args.MatViews
    logger.object_names = mat_views.split(",")
    logger.object_type = "materialized_views"

if args.Type == "publication" or args.Type == "publication_with_acl_owner":
    no_acl_no_owner = False
    if args.Type == "publication_with_acl_owner":
        no_acl_no_owner = True
    logger.publish_or_depublish = "publication"

    try:
        publish(
            src_conn_string,
            dst_conn_string,
            logger.path_to_log_file,
            schemas=schemas,
            tables=tables,
            views=views,
            materialized_views=mat_views,
            force=True,
            no_acl_no_owner=no_acl_no_owner,
        )
        logger.success = True
    except Exception:
        logger.success = False
    finally:
        logger.insert_log_row()
else:
    logger.publish_or_depublish = "depublication"
    try:
        depublish(
            src_conn_string,
            dst_conn_string,
            logger.path_to_log_file,
            schemas=schemas,
            tables=tables,
            views=views,
            materialized_views=mat_views,
            force=True,
        )
        logger.success = True
    except Exception:
        logger.success = False
    finally:
        logger.insert_log_row()
