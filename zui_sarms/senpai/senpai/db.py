import logging
import os
import pymongo
from sshtunnel import SSHTunnelForwarder

from senpai.utils import load_config

log = logging.getLogger(__name__)

# logging.basicConfig(level=logging.DEBUG)


CONFIG = load_config(os.path.join(os.getcwd(), "config.json"))


def mongo_wrapper(f):
    def wrapper(*args, **kwargs):
        MONGO_USER = CONFIG["MONGO_USER"]
        MONGO_PKEY = os.path.join(os.getcwd(), "mongo_pkey")
        MONGO_HOST = CONFIG["MONGO_HOST"]
        MONGO_DB = CONFIG["MONGO_DB"]
        MONGO_PORT = CONFIG["MONGO_PORT"]

        with SSHTunnelForwarder(MONGO_HOST,
                                ssh_username=MONGO_USER,
                                ssh_pkey=MONGO_PKEY,
                                remote_bind_address=('127.0.0.1', MONGO_PORT)) as tunnel:
            client = pymongo.MongoClient('127.0.0.1',
                                         tunnel.local_bind_port)  # server.local_bind_port is assigned local port
            db = client.get_database(MONGO_DB)

            f(*args, **kwargs, db=db)

    return wrapper
