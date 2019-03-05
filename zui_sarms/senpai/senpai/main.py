from sshtunnel import SSHTunnelForwarder
import os
import pymongo
import pprint
import logging

from senpai.db import mongo_wrapper

log = logging.getLogger(__name__)
# logging.basicConfig(level=logging.DEBUG)


@mongo_wrapper
def main(db=None):
    """

    :param db:
    :return:
    """
    log.info("Connecting to mongoDB %s", db)

    col = db.get_collection("test")  # type: pymongo.collection.Collection

    for i in range(10):
        col.insert_one({"data": i})

    print("Done")

    for i in col.find():
        print(i)

    print("Done")


if __name__ == '__main__':
    main()
