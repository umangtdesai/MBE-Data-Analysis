import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

############################################
# grab_income.py
# Script for collecting Chicago Census Socioeconomic Indicators
############################################

class grab_income(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.income']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = income.writes[0]

        # ---[ Grab Data ]-------------------------------------------
        url = 'https://api.datausa.io/api/?sort=desc&show=geo&required=income&sumlevel=tract&year=all&where=geo%3A16000US1714000'
        db = pd.read.csv(url).to_json()
        json_response = json.loads(db)


        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('income')
        repo.createCollection('income')
        repo[repo_name].insert_many(json_response)
        repo[repo_name].metadata({'complete': True})

        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

        #@staticmethod
        #def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):