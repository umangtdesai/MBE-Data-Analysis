#import urllib.request
#import json
import dml
import prov.model
import datetime
import uuid
import math

class nearest_departments(dml.Algorithm):
    contributor = 'npearce'
    reads = ['npearce.boston_fire_departments', 'npearce.boston_public_schools', 'npearce.boston_nonpublic_schools']
    writes = ['npearce.ps_departments', 'npearce.nps_departments']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('npearce', 'npearce')

        # Drop and create mongo collections
        repo.dropCollection('ps_departments')
        repo.createCollection('ps_departments')
        repo.dropCollection('nps_departments')
        repo.createCollection('nps_departments')
        
        # Read public school locations
        pss = repo['npearce.boston_public_schools'].find()
        
        # Collect distances to nearest department in dists
        dists = []
        for ps in pss:
            coords = ps['coordinates']
            query = {'coordinates': {'$near': coords}}
            nearest = repo['npearce.boston_fire_departments'].find_one(query)
            
            x1, y1 = coords
            x2, y2 = nearest['coordinates']
            dist = math.sqrt((x1 - x2)**2 + (y1 - y2)**2)
            dists.append(dist)
        repo['npearce.ps_departments'].insert_many(dists)
        
        repo['npearce.ps_departments'].metadata({'complete':True})
        print(repo['npearce.ps_departments'].metadata())
        
        # Read non public school locations
        npss = repo['npearce.boston_nonpublic_schools'].find()
        
        # Collect distances to nearest department in dists
        dists = []
        for nps in npss:
            coords = nps['coordinates']
            query = {'coordinates': {'$near': coords}}
            nearest = repo['npearce.boston_fire_departments'].find_one(query)
            
            x1, y1 = coords
            x2, y2 = nearest['coordinates']
            dist = math.sqrt((x1 - x2)**2 + (y1 - y2)**2)
            dists.append(dist)
        repo['npearce.nps_departments'].insert_many(dists)
        
        repo['npearce.nps_departments'].metadata({'complete':True})
        print(repo['npearce.nps_departments'].metadata())
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {'start':startTime, 'end':endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''
               
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