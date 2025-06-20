import os
from datetime import datetime

from pg_publisher.config import get_config

config = get_config()

LOG_FILE_PATH = os.path.abspath(
    os.path.join(
        config.get("DEFAULT", "logDir"),
        "log{date_time}.log".format(
            date_time=datetime.now().strftime("%Y-%m-%d-%H-%M")
        ),
    )
)


class PublisherLogger:
    def __init__(self, conn):
        self._src_db = None  # need
        self._dst_db = None  # need
        self.publish_type = None  # need
        self._success = False
        self._object_names = None
        self._object_type = None
        self._error_messages = []
        self._dependences_warning = []
        self._view_dependences = []
        self.conn = conn
        self.user = os.environ.get("USER", os.environ.get("USERNAME"))  # need
        self.create_logging_schema()  # need
        self.create_logging_table()  # need

    def create_logging_schema(self):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS logging;")

    def create_logging_table(self):
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """CREATE TABLE IF NOT EXISTS logging.logging(
                        id serial PRIMARY KEY,
                        date timestamp WITH time zone DEFAULT now(),
                        utilisateur varchar(50),
                        src_db_service_name varchar(50),
                        dst_db_service_name varchar(50),
                        type_objet varchar(20),
                        nom_objets varchar(1000),
                        succes boolean,
                        log_complet varchar(150), --chemin vers fichier de log
                        message_erreur varchar(15000),
                        commande varchar(1500),
                        publier_depublier varchar(50),
                        warning_dependances varchar(15000),
                        vues_dependante varchar(10000)
                      );
                    """
                )

    @property
    def src_db(self):
        return self._src_db

    @src_db.setter
    def src_db(self, src_db):
        self._src_db = src_db

    @property
    def dst_db(self):
        return self._dst_db

    @dst_db.setter
    def dst_db(self, dst_db):
        self._dst_db = dst_db

    @property
    def object_type(self):
        return self._object_type

    @object_type.setter
    def object_type(self, object_type):
        self._object_type = object_type

    @property
    def dependences_warning(self):
        return (
            ";".join(self._dependences_warning)
            if self._dependences_warning is not None
            else None
        )

    @dependences_warning.setter
    def dependences_warning(self, dependences_warning):
        self._dependences_warning = dependences_warning

    @property
    def view_dependences(self):
        return (
            ";".join(self._view_dependences)
            if self._view_dependences is not None
            else None
        )

    @view_dependences.setter
    def view_dependences(self, view_dependences):
        self._view_dependences = view_dependences

    @property
    def object_names(self):
        return ";".join(self._object_names) if self._object_names is not None else None

    @object_names.setter
    def object_names(self, object_names):
        self._object_names = object_names

    @property
    def error_count_messages(self):
        return len(self._error_messages) if self._error_messages is not None else None

    @property
    def error_messages(self):
        return self._error_messages

    @error_messages.setter
    def error_messages(self, error_messages):
        self._error_messages = error_messages

    @property
    def success(self):
        return self._success

    @success.setter
    def success(self, success):
        self._success = success

    def build_cmd_command(self):
        """
        "Schemas": "schemas",
        "Tables": "tables",
        "Vues": "vues",
        "Vues Matérialisées": "materialized_views"
        """
        cmd_command = "-ty={} -src_db={} -dst_db={} ".format(
            self.publish_type, self.src_db, self.dst_db
        )
        if self.object_type == "schemas":
            cmd_command += '-s="{}"'.format(self.object_names)
        elif self.object_type == "tables":
            cmd_command += '-t="{}"'.format(self.object_names)
        elif self.object_type == "views":
            cmd_command += '-v="{}"'.format(self.object_names)
        elif self.object_type == "materialized_views":
            cmd_command += '-mv="{}"'.format(self.object_names)
        else:
            raise Exception("object_type is not valid: {}".format(self.object_type))
        return cmd_command

    def insert_log_row(self):
        sql = """INSERT INTO logging.logging (utilisateur, src_db_service_name,
         dst_db_service_name, type_objet, nom_objets, succes, log_complet,
         message_erreur, commande, publier_depublier, warning_dependances, vues_dependante)
        VALUES (
        '{user}',
        '{src_db}',
        '{dst_db}',
        '{object_type}',
        '{object_names}',
        '{success}',
        '{path_to_log_file}',
        '{error_messages}',
        '{command_pour_publish_cron}',
        '{publish_type}',
        '{dependences_warning}',
        '{view_dependences}'
        )""".format(
            user=self.user,
            src_db=self.src_db,
            dst_db=self.dst_db,
            object_type=self.object_type,
            object_names=self.object_names,
            success=self.success,
            path_to_log_file=LOG_FILE_PATH,
            error_messages=",".join(self.error_messages),
            command_pour_publish_cron=self.build_cmd_command(),
            publish_type=self.publish_type,
            dependences_warning=self.dependences_warning,
            view_dependences=self.view_dependences,
        )
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
