from pgtoolkit.service import find, parse
from psycopg2 import connect


def get_service_file():
    try:
        service_file = find()
    except:
        raise SystemExit(
            "Absence de fichier pgservice.conf, la variable d'environnement est-elle définie?"
        )
    return service_file


def get_services():
    service_file = get_service_file()
    with open(service_file) as f:
        service_file = parse(f)
        conf = service_file.config
        services = [each_section for each_section in conf.sections()]
    return services


def get_conn_from_service_name(service_name):
    service_file = get_service_file()
    with open(service_file) as f:
        service_file = parse(f)
        host = service_file[service_name]["host"]
        dbname = service_file[service_name]["dbname"]
        user = service_file[service_name]["user"]
        port = service_file[service_name]["port"]
        password = service_file[service_name]["password"]

        connection_string = "host={host} dbname={dbname} user={user} password={password} port={port}".format(
            host=host, dbname=dbname, user=user, password=password, port=port
        )
        connection = connect(
            host=host, dbname=dbname, user=user, password=password, port=port
        )
    return {"conn": connection, "conn_str": connection_string}
