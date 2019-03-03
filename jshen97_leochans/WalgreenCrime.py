import dml
import datetime
import json
import prov.model
import uuid
from urllib.request import urlopen


class WalgreenCrime(dml.Algorithm):

    contributor = "jshen97_leochans"
    reads = ['jshen97_leochans.walgreen', 'jshen97_leochans.refinedCrime']
    writes = ['jshen97_leochans.walgreenCrime']

    @staticmethod
    def execute(trial=False):

        start_time = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('leochans_jshen97', 'leochans_jshen97')


        # TODO merge refinedCrime and walgreen


        repo.logout()

        end_time = datetime.datetime.now()

        return {"start": start_time, "end": end_time}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), start_time=None, end_time=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('leochans_jshen97', 'leochans_jshen97')


        # TODO complete prov


        repo.logout()

        return doc
