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


# RIDERSHIP_STATS = pd.read_json('http://data.cityofchicago.org/resource/5neh-572f.json')
# print('grabbed ridership')
# NEIGHBORHOODS = geopandas.read_file('http://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=GeoJSON')
# CENSUS = pd.read_json('http://data.cityofchicago.org/resource/kn9c-c2s2.json')
# print('grabbed census')
# LOCATIONS = geopandas.read_file('http://datamechanics.io/data/smithnj/CTA_RailStations.geojson')
# print('grabbed locations')
# CONGESTION = pd.read_json('https://data.cityofchicago.org/resource/m65n-ux8y.json')
# print('grabbed congestion')
#
# #  ALL_DATA = [RIDERSHIP_STATS, NEIGHBORHOODS, CENSUS, INCOME, LOCATIONS]
# ALL_DATA = [RIDERSHIP_STATS, NEIGHBORHOODS, CENSUS, LOCATIONS, CONGESTION]
#
# print('RIDERSHIP')
# print(list(RIDERSHIP_STATS))
# print('NEIGH')
# print(list(NEIGHBORHOODS))
# print('CENSUS')
# print(list(CENSUS))
# print('LOCATIONS')
# print(list(LOCATIONS))
# print('CONGESTION')
# print(list(CONGESTION))
import fiona

startTime = datetime.datetime.now()

# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.neighborhoods'
# ---[ Grab Data ]-------------------------------------------
df = geopandas.read_file('https://data.cityofchicago.org/api/geospatial/bbvz-uum9?method=export&format=GeoJSON')
csv = df.to_file('testing', driver='CSV')
# ---[ MongoDB Insertion ]-------------------------------------------
#repo.dropCollection('neighborhoods')
#repo.createCollection('neighborhoods')
#print('done')
#repo[repo_name].insert_many(loaded)
#repo[repo_name].metadata({'complete': True})
# ---[ Finishing Up ]-------------------------------------------
#print(repo[repo_name].metadata())
#repo.logout()
#endTime = datetime.datetime.now()
#return {"start": startTime, "end": endTime}



---

startTime = datetime.datetime.now()

# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.travelstats'
# ---[ Grab Data ]-------------------------------------------
df = pd.read_csv('/Users/nathaniel/Desktop/smithnj/CTA_Ridership_Totals.csv') #TODO CHANGE TO WEB LINK
# ---[ Begin Transformation ]--------------------------------
df['date'] = pd.to_datetime(df['date']) # Convert "date" index to datetime
df.index = df['date'] # set dataframe index to date
monthly_sums = df.groupby([df.index.month, df['station_id'], df['stationname']]).sum() # find total num of travelers per station per month
monthly_sums['rides'] = monthly_sums['rides'].apply(lambda x: x/4) # divide rides by four as data is over a four year period
# ---[ Write Data to JSON ]----------------------------------
monthly_sums = monthly_sums.reset_index()
df_json = monthly_sums.to_json(orient='records')
loaded = json.loads(df_json)
# ---[ MongoDB Insertion ]-------------------------------------------
repo.dropCollection('travelstats')
repo.createCollection('travelstats')
print('done')
repo[repo_name].insert_many(loaded)
repo[repo_name].metadata({'complete': True})
# ---[ Finishing Up ]-------------------------------------------
print(repo[repo_name].metadata())
repo.logout()
endTime = datetime.datetime.now()
#return {"start": startTime, "end": endTime}