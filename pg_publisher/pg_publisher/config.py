import configparser
import os.path
import sys
from pkg_resources import resource_filename


def get_config():
    if getattr(sys, "frozen", False):
        application_path = os.path.dirname(sys.executable)
        config_path = os.path.join(application_path, "conf.ini")
    else:
        config_path = resource_filename("pg_publisher", "conf.ini")

    config = configparser.ConfigParser()
    config.read(config_path)
    return config
