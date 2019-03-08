import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

############################################
# get_stationlocation.py
# Script for collecting CTA L station location
############################################

class get_stationlocation(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.stationlocation']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.cs506
        repo.authenticate('smithnj', 'bOstonuniv!!')
        repo_name = stationlocation.writes[0]

        # ---[ Grab Data ]-------------------------------------------
        url = 'https://data.cityofchicago.org/resource/5neh-572f.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        jsondata = json.loads(data)
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('ctastations')
        repo.createCollection('ctastations')
        repo[repo_name].insert_many(r)
        repo[repo_name].metadata({'complete': True})

        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}