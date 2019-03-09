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
CENSUS = pd.read_json('http://data.cityofchicago.org/resource/kn9c-c2s2.json')
# print('grabbed census')
LOCATIONS = geopandas.read_file('https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON')
print('grabbed locations')
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
# print(CENSUS.head(10))
# print('LOCATIONS')
# print(list(LOCATIONS))
# print(LOCATIONS.head(3))
# print('CONGESTION')
# print(list(CONGESTION))
import fiona

startTime = datetime.datetime.now()
# ---[ Connect to Database ]---------------------------------
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('smithnj', 'smithnj')
repo_name = 'smithnj.create_completeareas'
# ---[ Grab Data ]-------------------------------------------
communityareas = geopandas.read_file('https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON')
communityareas['area_numbe'] = communityareas['area_numbe'].convert_objects(convert_numeric=True)
census = pd.read_json('http://data.cityofchicago.org/resource/kn9c-c2s2.json')
merged = communityareas.merge(census, left_on='area_numbe', right_on='ca')