import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geopandas

############################################
# grab_neighborhoods.py
# Script for collecting Chicago Community Areas
############################################

class grab_neighborhoods(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.communityareas']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.communityareas'
        # ---[ Grab Data ]-------------------------------------------
        df = geopandas.read_file(
            'https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON').to_json()
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('communityareas')
        repo.createCollection('communityareas')
        print('done')
        repo[repo_name].insert_one(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}









        #@staticmethod
        #def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
    #TODO Add grab neighborhoods provenance
