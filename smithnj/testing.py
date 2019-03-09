import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import geopandas
from pymongo import MongoClient, GEO2D
import geojson

startTime = datetime.datetime.now()
# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.congestion'
# ---[ Grab Data ]-------------------------------------------
df = pd.read_csv('https://data.cityofchicago.org/api/views/t2qc-9pjd/rows.csv?accessType=DOWNLOAD').to_json(orient='records')
loaded = json.loads(df)
# ---[ MongoDB Insertion ]-------------------------------------------
repo.dropCollection('congestion')
repo.createCollection('congestion')
print('done')
repo[repo_name].insert_many(loaded)
repo[repo_name].metadata({'complete': True})
# ---[ Finishing Up ]-------------------------------------------
print(repo[repo_name].metadata())
repo.logout()
endTime = datetime.datetime.now()
#return {"start": startTime, "end": endTime}