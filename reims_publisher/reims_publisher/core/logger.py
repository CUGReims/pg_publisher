import logging
import os
from datetime import datetime
import configparser
from pkg_resources import resource_filename

config = configparser.ConfigParser()
if os.path.exists("conf.ini"):
    config.read("conf.ini")
else:
    config.read(resource_filename("reims_publisher", "conf.ini"))


class PublisherLogger:
    def __init__(self, conn):
        self.path_to_log_file = None  # need
        self._src_db = None  # need
        self._dst_db = None  # need
        self.publish_type = None  # need
        self._success = False
        self._object_names = None
        self._object_type = None
        self._error_messages = []
        self.conn = conn
        self.create_log_file()
        self.user = os.environ.get("USER", os.environ.get("USERNAME"))  # need
        self.create_logging_schema()  # need
        self.create_logging_table()  # need

    def create_log_file(self):
        date_time = datetime.now().strftime("%Y-%m-%d-%H-%M")
        self.path_to_log_file = "{dir}log{date_time}.log".format(
            dir=config.get("DEFAULT", "logDir"), date_time=date_time
        )
        logging.basicConfig(filename=self.path_to_log_file, level=logging.INFO)

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
                        messager_erreur varchar(15000),
                        commande varchar(1500),
                        publier_depublier varchar(50)
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
    def object_names(self):
        return ",".join(self._object_names) if self._object_names is not None else None

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
            cmd_command += "-schemas={}".format(self.object_names)
        elif self.object_type == "tables":
            cmd_command += "-tables={}".format(self.object_names)
        elif self.object_type == "views":
            cmd_command += "-views={}".format(self.object_names)
        elif self.object_type == "materialized_views":
            cmd_command += "-materialized_views={}".format(self.object_names)
        else:
            raise Exception("object_type is not valid: {}".format(self.object_type))
        return cmd_command

    def insert_log_row(self):
        sql = """INSERT INTO logging.logging (utilisateur, src_db_service_name, dst_db_service_name,
         type_objet, nom_objets, succes, log_complet, messager_erreur, commande, publier_depublier)
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
        '{publish_type}'
        )""".format(
            user=self.user,
            src_db=self.src_db,
            dst_db=self.dst_db,
            object_type=self.object_type,
            object_names=self.object_names,
            success=self.success,
            path_to_log_file=self.path_to_log_file,
            error_messages=",".join(self.error_messages),
            command_pour_publish_cron=self.build_cmd_command(),
            publish_type=self.publish_type,
        )
        with self.conn:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
