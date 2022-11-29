import subprocess
from typing import List, Optional


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
    log_file_path: str,
    schemas: Optional[List[str]] = None,
    tables: Optional[List[str]] = None,
    views: Optional[List[str]] = None,
    materialized_views: Optional[List[str]] = None,
    force: Optional[bool] = True,
):
    with open(log_file_path, "a") as f:
        receiver = subprocess.Popen(
            ["psql", "--single-transaction", dst_conn_string],
            stdin=subprocess.PIPE,
            stdout=f,
            stderr=subprocess.STDOUT,
            encoding="utf8",
        )

        receiver.stdin.write("\\set ON_ERROR_STOP on\n")
        receiver.stdin.flush()

        if schemas:
            for schema in schemas:
                sql_query = (
                    "DROP SCHEMA IF EXISTS {} CASCADE;".format(schema)
                    if force
                    else "DROP SCHEMA IF EXISTS {};".format(schema)
                )
                receiver.stdin.write(sql_query)
                receiver.stdin.flush()
            emitter = subprocess.Popen(
                ["pg_dump"]
                + [arg for schema in schemas for arg in ["-n", schema]]
                + [src_conn_string],
                stdout=receiver.stdin,
                stderr=f,
            )
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
            emitter = subprocess.Popen(
                ["pg_dump"]
                + [arg for table in tables for arg in ["-t", table]]
                + [src_conn_string],
                stdout=receiver.stdin,
                stderr=f,
            )
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
            emitter = subprocess.Popen(
                ["pg_dump"]
                + [arg for view in views for arg in ["-t", view]]
                + [src_conn_string],
                stdout=receiver.stdin,
                stderr=f,
            )
            emitter.communicate()

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
                receiver.stdin.flush()
            emitter = subprocess.Popen(
                ["pg_dump"]
                + [arg for mat_view in materialized_views for arg in ["-t", mat_view]]
                + [src_conn_string],
                stdout=receiver.stdin,
                stderr=f,
            )
            emitter.communicate()

        receiver.communicate()

    with open(log_file_path, "r") as f:
        for line in f:
            continue

    returncode = receiver.returncode
    if returncode == 0:
        return
    if returncode == 1:
        raise PsqlFatalError(
            f"Erreur fatale: {line}\nVoir le fichier {log_file_path} pour plus de détails."
        )
    if returncode == 2:
        raise PsqlConnectionLostError(
            f"Connection lost: {line}\nVoir le fichier {log_file_path} pour plus de détails."
        )
    if returncode == 3:
        raise PsqlOperationalError(
            f"La restauration a échouée: {line}\nVoir le fichier {log_file_path} pour plus de détails."
        )
