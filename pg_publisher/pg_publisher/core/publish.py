import logging
import subprocess
from typing import List, Optional

from pg_publisher.core.logger import LOG_FILE_PATH

LOG = logging.getLogger(__name__)


class PsqlError(Exception):
    pass


class PsqlFatalError(PsqlError):
    pass


class PsqlConnectionLostError(PsqlError):
    pass


class PsqlOperationalError(PsqlError):
    pass


def publish(
    src_conn_string: str,
    dst_conn_string: str,
    schemas: Optional[List[str]] = None,
    tables: Optional[List[str]] = None,
    views: Optional[List[str]] = None,
    materialized_views: Optional[List[str]] = None,
    no_acl_no_owner: Optional[bool] = False,
    force: Optional[bool] = True,
):
    with open(LOG_FILE_PATH, "a") as f:
        receiver = subprocess.Popen(
            ["psql", "--single-transaction", dst_conn_string],
            stdin=subprocess.PIPE,
            stdout=f,
            stderr=subprocess.STDOUT,
            encoding="utf8",
        )

        receiver.stdin.write("\\set ON_ERROR_STOP on\n")
        receiver.stdin.flush()
        dump_command = ["pg_dump"]
        if no_acl_no_owner:
            dump_command.append("-Ox")

        if schemas:
            for schema in schemas:
                sql_query = (
                    "DROP SCHEMA IF EXISTS {} CASCADE;".format(schema)
                    if force
                    else "DROP SCHEMA IF EXISTS {};".format(schema)
                )
                receiver.stdin.write(sql_query)
                receiver.stdin.flush()
            dump_command += [arg for schema in schemas for arg in ["-n", schema]]
            dump_command += [src_conn_string]
            LOG.info("Running dump command: %s", dump_command)
            emitter = subprocess.Popen(dump_command, stdout=receiver.stdin, stderr=f)
            emitter.communicate()

        if tables:
            for table in tables:
                sql_query = (
                    "DROP TABLE IF EXISTS {} CASCADE;".format(table)
                    if force
                    else "DROP TABLE IF EXISTS {};".format(table)
                )
                receiver.stdin.write(sql_query)
                receiver.stdin.flush()
            dump_command += [arg for table in tables for arg in ["-t", table]]
            dump_command += [src_conn_string]
            LOG.info("Running dump command: %s", dump_command)
            emitter = subprocess.Popen(dump_command, stdout=receiver.stdin, stderr=f)
            emitter.communicate()

        if views:
            for view in views or []:
                sql_query = (
                    "DROP VIEW IF EXISTS {} CASCADE;".format(view)
                    if force
                    else "DROP VIEW IF EXISTS {};".format(view)
                )
                receiver.stdin.write(sql_query)
                receiver.stdin.flush()
            dump_command += [arg for view in views for arg in ["-t", view]]
            dump_command += [src_conn_string]
            LOG.info("Running dump command: %s", dump_command)
            emitter = subprocess.Popen(dump_command, stdout=receiver.stdin, stderr=f)
            emitter.communicate()

        if materialized_views:
            for mat_view in materialized_views or []:
                sql_query = (
                    "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE;".format(mat_view)
                    if force
                    else "DROP MATERIALIZED VIEW IF EXISTS {};".format(mat_view)
                )
                receiver.stdin.write(sql_query)
                receiver.stdin.flush()
            dump_command += [
                arg for mat_view in materialized_views for arg in ["-t", mat_view]
            ]
            dump_command += [src_conn_string]
            LOG.info("Running dump command: %s", dump_command)
            emitter = subprocess.Popen(dump_command, stdout=receiver.stdin, stderr=f)
            emitter.communicate()

        receiver.communicate()

    error_message = ""
    error_found = False
    with open(LOG_FILE_PATH, "r") as f:
        for line in f:
            if "ERROR:" in line:
                error_found = True
            if error_found:
                error_message += line
    if error_found:
        error_message = str(
            error_message[error_message.rindex("ERROR:") :]  # noqa
        )  # keep last ERROR only
    returncode = receiver.returncode
    if returncode == 0:
        return
    if returncode == 1:
        raise PsqlFatalError(f"Erreur fatale: {error_message}")
    if returncode == 2:
        raise PsqlConnectionLostError(f"Connection lost: {error_message}")
    if returncode == 3:
        raise PsqlOperationalError(f"La restauration a échouée: {error_message}")
