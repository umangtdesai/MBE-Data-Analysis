import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

############################################
# grab_censusstats.py
# Script for collecting Chicago Census Socioeconomic Indicators
############################################

class grab_censusstats(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.census']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.census'
        # ---[ Grab Data ]-------------------------------------------
        df = pd.read_json('http://data.cityofchicago.org/resource/kn9c-c2s2.json').to_json(orient='records')
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('census')
        repo.createCollection('census')
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}









        #@staticmethod
        #def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):