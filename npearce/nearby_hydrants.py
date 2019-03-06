#import urllib.request
#import json
import dml
import prov.model
import datetime
import uuid

from bson.son import SON

class nearby_hydrants(dml.Algorithm):
    contributor = 'npearce'
    reads = ['npearce.boston_fire_hydrants', 'npearce.boston_public_schools', 'npearce.boston_nonpublic_schools']
    writes = ['npearce.ps_hydrants', 'npearce.nps_hydrants']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('npearce', 'npearce')

        # Drop and create mongo collections
        repo.dropCollection('ps_hydrants')
        repo.createCollection('ps_hydrants')
        repo.dropCollection('nps_hydrants')
        repo.createCollection('nps_hydrants')
        
        # Read public school locations
        pss = repo['npearce.boston_public_schools'].find()
        
        # Collect nearby hydrant counts in counts
        counts = []
        for ps in pss:
            coords = ps['coordinates']
            query = {'coordinates': SON([('$near', coords), 
                                         ('$maxDistance', 0.00724637681159)])}
            c = repo['npearce.boston_fire_hydrants'].find(query).count()
            counts.append(c)
        repo['npearce.ps_hydrants'].insert_many(counts)
        
        repo['npearce.ps_hydrants'].metadata({'complete':True})
        print(repo['npearce.ps_hydrants'].metadata())
        
        # Read non public school locations
        npss = repo['npearce.boston_nonpublic_schools'].find()
        
        # Collect nearby hydrant counts in counts
        counts = []
        for nps in npss:
            coords = nps['coordinates']
            query = {'coordinates': SON([('$near', coords), 
                                         ('$maxDistance', 0.00724637681159)])}
            c = repo['npearce.boston_fire_hydrants'].find(query).count()
            counts.append(c)
        repo['npearce.nps_hydrants'].insert_many(counts)
        
        repo['npearce.nps_hydrants'].metadata({'complete':True})
        print(repo['npearce.nps_hydrants'].metadata())
        
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