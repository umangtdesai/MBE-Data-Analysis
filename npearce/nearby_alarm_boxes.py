#import urllib.request
#import json
import dml
import prov.model
import datetime
import uuid

from bson.son import SON

class nearby_alarm_boxes(dml.Algorithm):
    contributor = 'npearce'
    reads = ['npearce.boston_fire_alarm_boxes', 'npearce.boston_public_schools', 'npearce.boston_nonpublic_schools']
    writes = ['npearce.ps_alarm_boxes', 'npearce.nps_alarm_boxes']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('npearce', 'npearce')

        # Drop and create mongo collections
        repo.dropCollection('ps_alarm_boxes')
        repo.createCollection('ps_alarm_boxes')
        repo.dropCollection('nps_alarm_boxes')
        repo.createCollection('nps_alarm_boxes')
        
        # Read public school locations
        pss = repo['npearce.boston_public_schools'].find()
        
        # Collect nearby alarm box counts in counts
        counts = []
        for ps in pss:
            coords = ps['coordinates']
            query = {'coordinates': SON([('$near', coords), 
                                         ('$maxDistance', 0.00724637681159)])}
            c = repo['npearce.boston_fire_alarm_boxes'].find(query).count()
            counts.append(c)
        repo['npearce.ps_alarm_boxes'].insert_many(counts)
        
        repo['npearce.ps_alarm_boxes'].metadata({'complete':True})
        print(repo['npearce.ps_alarm_boxes'].metadata())
        
        # Read non public school locations
        npss = repo['npearce.boston_nonpublic_schools'].find()
        
        # Collect nearby alarm box counts in counts
        counts = []
        for nps in npss:
            coords = nps['coordinates']
            query = {'coordinates': SON([('$near', coords), 
                                         ('$maxDistance', 0.00724637681159)])}
            c = repo['npearce.boston_fire_alarm_boxes'].find(query).count()
            counts.append(c)
        repo['npearce.nps_alarm_boxes'].insert_many(counts)
        
        repo['npearce.nps_alarm_boxes'].metadata({'complete':True})
        print(repo['npearce.nps_alarm_boxes'].metadata())
        
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