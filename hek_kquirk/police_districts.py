import geopandas
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import shapefile
import requests
import zipfile
import io

class police_districts(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = []
    writes = ['hek_kquirk.police_stations']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')
        
        l = []
        url = 'https://dataverse.harvard.edu/api/access/datafile/:persistentId/?persistentId=doi:10.7910/DVN/JZV6ON/BGTJS7&version=1.1'
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall() 

        reader = shapefile.Reader("Police_Districts.shp")
        fields = reader.fields[1:]
        field_names = [field[0] for field in fields]
        buf = []

        for sr in reader.shapeRecords():
            atr = dict(zip(field_names, sr.record))
            geom = sr.shape.__geo_interface__
            buf.append(dict(type="Feature", geometry=geom, properties=atr))
        
        geojson = open("Police_Districts.json", "w")
        #geojson.write(json.dumps({"type": "FeatureCollection","features": buf}, sort_keys=True, indent=2) + "\n")
        geojson.write(json.dumps(buf, sort_keys=True, indent=2) + "\n")
        geojson.close()

        geojson = open("Police_Districts.json", "r")
        repo.dropCollection("police_districts")
        repo.createCollection("police_districts")
        repo['hek_kquirk.police_districts'].insert_many(json.loads(geojson.read()))
        repo['hek_kquirk.police_districts'].metadata({'complete':True})
        print(repo['hek_kquirk.police_districts'].metadata())
        repo.logout()

        endTime = datetime.datetime.now()

        geojson.close()
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
