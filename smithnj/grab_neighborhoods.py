import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

############################################
# grab_neighborhoods.py
# Script for collecting Chicago neighborhoods
############################################

class grab_neighborhoods(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.neighborhoods']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()
        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.neighborhoods'
        # ---[ Grab Data ]-------------------------------------------
        df = pd.read_csv('http://datamechanics.io/data/smithnj/Neighborhoods_2012b.csv').to_json(orient='records')
        loaded = json.loads(df)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('neighborhoods')
        repo.createCollection('neighborhoods')
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
