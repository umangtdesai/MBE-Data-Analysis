try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen
import json
import dml
import prov.model
import datetime
import uuid

class ProjectCVS(dml.Algorithm):
    contributor = "jshen97_leochans"
    reads = []
    writes = []

    @staticmethod
    def execute(trail = False):

        startTime = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jshen97_leochans', 'jshen97_leochans')

        #retrieve CVS store location through Google Places Search APIs
        service = dml.auth["googleAPI"]["service"]

        #required parameters to specify for place searching @see Places API documentation
        key = "key="+dml.auth["googleAPI"]["key"]
        input = "input=CVS%20Boston,%20MA&"
        inputtype = "inputtype=textquery&"

        #fields that are interesting or might be useful
        fields = "fields=place_id,formatted_address,geometry/location," \
                 "types,atmosphere,user_rating_total&"

        url = service+input+inputtype+fields+key
        response = json.load(urlopen(url))
        res_dump = json.dump(response, sort_keys=True,indent=2)
        repo.dropCollection('cvs')
        repo.createCollection('cvs')
        repo['jshen97_leochans.cvs'].insert_many(response)
        repo['jshen97_leochans.cvs'].metadata({'complete': True})
        print(repo['jshen97_leochans.cvs'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime,"end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # TODO review definition of provenance and learn how to use prov library
        return doc
