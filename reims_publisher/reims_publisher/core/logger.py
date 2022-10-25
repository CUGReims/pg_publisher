import logging
import os
from datetime import datetime


class PublisherLogger:
    def __init__(self, conn):
        self._src_db = None
        self._dst_db = None
        self._success = False
        self._object_names = None
        self._object_type = None
        self._fail_reason = None
        self.conn = conn
        self.user = os.environ.get("USER", os.environ.get("USERNAME"))
        self.create_log_file()
        self.create_logging_schema()
        self.create_logging_table()

    def create_log_file(self):
        date_time = datetime.now().strftime("%Y-%m-%d")
        self._path_to_log_file = "/tmp/log{date_time}.log".format(date_time=date_time)
        logging.basicConfig(filename=self._path_to_log_file, level=logging.INFO)

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
                        raison_echec varchar(150)
                      );
                    """
                )

    @property
    def src_db(self):
        return self._object_type

    @src_db.setter
    def src_db(self, src_db):
        self._src_db = src_db

    @property
    def src_db(self):
        return self._src_db

    @src_db.setter
    def src_db(self, src_db):
        self._src_db = src_db

    @property
    def object_type(self):
        return self._object_type

    @object_type.setter
    def object_type(self, object_type):
        self._object_type = object_type

    @property
    def object_names(self):
        return self._object_type

    @object_names.setter
    def object_names(self, object_names):
        self._object_names = ",".join(object_names)

    @property
    def fail_reason(self):
        return self._object_type

    @fail_reason.setter
    def fail_reason(self, fail_reason):
        self.fail_reason = fail_reason

    @property
    def success(self):
        return self._success

    @success.setter
    def success(self, success):
        self._success = success

    def insert_log_row(self):
        sql = """INSERT INTO logging.logging (utilisateur, src_db_service_name, dst_db_service_name,
         type_objet, nom_objets, succes, log_complet, raison_echec)
        VALUES (
        '{user}',
        '{src_db}',
        '{dst_db}',
        '{object_type}',
        '{object_names}',
        '{success}',
        '{path_to_log_file}',
        '{fail_reason}'
        )""".format(
            user=self.user,
            src_db=self._src_db,
            dst_db=self._dst_db,
            object_type=self._object_type,
            object_names=self._object_names,
            success=self._success,
            path_to_log_file=self._path_to_log_file,
            fail_reason=self._fail_reason,
        )
        cursor = self.conn.cursor()
        cursor.execute(sql)
        self.conn.commit()
