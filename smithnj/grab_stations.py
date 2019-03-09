import urllib.request
import json
import dml
import prov.model
import datetime
import geopandas

############################################
# grab_stations.py
# Script for collecting CTA L station location
############################################

class grab_stations(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.stations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.stations'
        # ---[ Grab Data ]-------------------------------------------
        url = 'http://datamechanics.io/data/CTA_RailStations.csv'
        df = pd.read_csv('http://datamechanics.io/data/smithnj/CTA_RailStations.csv').to_json(orient='records')
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('stations')
        repo.createCollection('stations')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}









        #@staticmethod
        #def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
    #TODO insert provenance for grab_stations