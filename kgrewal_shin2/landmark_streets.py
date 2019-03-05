# Removes streets with landmarks on them from the street name list
import dml
from kgrewal_shin2 import transformations
import urllib.request
import requests
import json
import prov.model
import datetime
import uuid
from bson import ObjectId


def get_street(s):
    if "Bounded by" not in s:
        street = s.split(" ", 1)
        if street[0].isdigit():
            street = street[1]
        else:
            street = s

    else:
        street = s.split("Bounded by")[1]

    return street

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

r = repo['kgrewal_shin2.street_names'].find()

X = {}
for i in len(r):
    X.update({i: r[i]})
print(X)



# r = repo['kgrewal_shin2.landmarks'].find()
# Y = {}
# for z in r:
#     Y.update(z)

# Y = Y['features']
#
# s = json.dumps(Y, sort_keys=True, indent=2)
# print(s)