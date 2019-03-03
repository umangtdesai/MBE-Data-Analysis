import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import bson.code

class bpd_employee_earnings(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.boston_employee_earnings']
    writes = ['hek_kquirk.bpd_employee_earnings']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("bpd_employee_earnings")
        repo.createCollection("bpd_employee_earnings")

        bpd_earnings = repo['hek_kquirk.boston_employee_earnings'].find({'DEPARTMENT NAME': 'Boston Police Department'})
        repo['hek_kquirk.bpd_employee_earnings'].insert_many(bpd_earnings)
        
        repo['hek_kquirk.bpd_employee_earnings'].metadata({'complete':True})
        print(repo['hek_kquirk.bpd_employee_earnings'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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
