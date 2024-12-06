import subprocess
from typing import List, Optional

from pg_publisher.core.logger import LOG_FILE_PATH


def depublish(
    dst_conn_string: str,
    schemas: Optional[List[str]] = None,
    tables: Optional[List[str]] = None,
    views: Optional[List[str]] = None,
    materialized_views: Optional[List[str]] = None,
    force: Optional[bool] = True,
):
    with open(LOG_FILE_PATH, "a") as f:
        receiver = subprocess.Popen(
            ["psql", dst_conn_string], stdout=f, stdin=subprocess.PIPE
        )
        if schemas:
            for schema in schemas:
                sql_query = (
                    "DROP SCHEMA IF EXISTS {} CASCADE;".format(schema).encode("utf8")
                    if force
                    else "DROP SCHEMA IF EXISTS {};".format(schema).encode("utf8")
                )
                receiver.stdin.write(sql_query)
        if tables:
            for table in tables:

                sql_query = (
                    "DROP TABLE IF EXISTS {} CASCADE;".format(table).encode("utf8")
                    if force
                    else "DROP TABLE IF EXISTS {};".format(table).encode("utf8")
                )
                receiver.stdin.write(sql_query)
        if views:
            for view in views or []:
                sql_query = (
                    "DROP VIEW IF EXISTS {} CASCADE;".format(view).encode("utf8")
                    if force
                    else "DROP VIEW IF EXISTS {};".format(view).encode("utf8")
                )
                receiver.stdin.write(sql_query)

        if materialized_views:
            for mat_view in materialized_views or []:
                sql_query = (
                    "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format(
                        mat_view
                    ).encode("utf8")
                    if force
                    else "DROP MATERIALIZED VIEW IF EXISTS {};".format(mat_view).encode(
                        "utf8"
                    )
                )
                receiver.stdin.write(sql_query)
        receiver.communicate()
