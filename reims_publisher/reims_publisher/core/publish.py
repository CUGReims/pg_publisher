import subprocess
from typing import List, Optional


def publish(
    src_conn_string: str,
    dst_conn_string: str,
    schemas: Optional[List[str]] = None,
    tables: Optional[List[str]] = None,
    views: Optional[List[str]] = None,
    force: Optional[bool] = False,
):
    receiver = subprocess.Popen(
        ["psql", dst_conn_string],
        stdin=subprocess.PIPE,
    )

    for schema in schemas:
        receiver.stdin.write(
            "DROP SCHEMA IF EXISTS {} CASCADE;".format(schema).encode("utf8")
        )

    subprocess.Popen(
        ["pg_dump"]
        + [arg for schema in schemas for arg in ["-n", schema]]
        + [src_conn_string],
        stdout=receiver.stdin,
        stderr=subprocess.PIPE,
    )

    receiver.communicate()
