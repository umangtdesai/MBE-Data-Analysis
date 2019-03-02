import dml
import datetime
import json
import prov.model
import pandas as pd
import uuid
from urllib.request import urlopen

class crime(dml.Algorithm):
    contributor = "jshen97_leochans"
    reads = []
    writes = ['jshen97_leochans.cvs','jshen97_leochans.walgreen',\
              'jshen97_leochans.7eleven','jshen97_leochans.crime']

    @staticmethod
    def execute(trail = False):

        startTime = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jshen97_leochans', 'jshen97_leochans')

        # retrieve CVS, Walgreen, and 7-Eleven info through Google Places Search APIs
        # @see https://developers.google.com/places/web-service/search
        service = dml.auth["googleAPI"]["service"]

        # required parameters to specify for place searching @see Places API documentation
        key = "key="+dml.auth["googleAPI"]["key"]
        input_cvs = "input=CVS%20Boston,%20MA&"
        input_walgreen = "input=Walgreen%20Boston,%20MA&"
        input_7eleven = "input=7-Eleven%20Boston,%20MA&"
        inputtype = "inputtype=textquery&"

        # fields that are interesting or might be useful
        fields = "fields=place_id,formatted_address,geometry/location," \
                 "types,atmosphere,user_rating_total&"

        # retrieve cvs
        url_cvs = service+input_cvs+inputtype+fields+key
        response_cvs = json.load(urlopen(url_cvs).read().decode('utf-8'))
        res_dump_cvs = json.dump(response_cvs, sort_keys=True, indent=2)
        repo.dropCollection('cvs')
        repo.createCollection('cvs')
        repo['jshen97_leochans.cvs'].insert_many(response_cvs)
        repo['jshen97_leochans.cvs'].metadata({'complete': True})
        print(repo['jshen97_leochans.cvs'].metadata())

        # retrieve walgreen
        url_walgreen = service+input_walgreen+inputtype+fields+key
        response_walgreen = json.load(urlopen(url_walgreen).read().decode('utf-8'))
        res_dump_walgreen = json.dump(response_walgreen, sort_keys=True, indent=2)
        repo.dropCollection('walgreen')
        repo.createCollection('walgreen')
        repo['jshen97_leochans.walgreen'].insert_many(response_walgreen)
        repo['jshen97_leochans.walgreen'].metadata({'complete': True})
        print(repo['jshen97_leochans.walgreen'].metadata())

        # retrieve 7-Eleven
        url_7eleven = service+input_7eleven+inputtype+fields+key
        response_7eleven = json.load(urlopen(url_7eleven).read().decode('utf-8'))
        res_dump_7eleven = json.dump(response_7eleven, sort_keys=True, indent=2)
        repo.dropCollection('7eleven')
        repo.createCollection('7eleven')
        repo['jshen97_leochans.7eleven'].insert_many(response_7eleven)
        repo['jshen97_leochans.7eleven'].metadata({'complete': True})
        print(repo['jshen97_leochans.7eleven'].metadata())

        # retrieve crime.csv
        url_crime = "http://datamechanics.io/data/crime.csv"
        # if using pandas older than 0.19.2, use a string io instead of passing the
        # url directly
        df_crime = pd.read_csv(url_crime)
        json_crime = json.load(df_crime.to_json(orient='records'))
        json_dump_7eleven = json.dump(json_crime, sort_keys=True, indent=2)
        repo.dropCollection('crime')
        repo.createCollection('crime')
        repo['jshen97_leochans.crime'].insert_many(json_crime)
        repo['jshen97_leochans.crime'].metadata({'complete': True})
        print(repo['jshen97_leochans.crime'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime,"end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''
        # TODO review definition of provenance and learn how to use prov library
        return doc
