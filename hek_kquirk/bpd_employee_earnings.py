import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class bpd_employee_earnings(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = []
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

        # Api call for employee earnings dataset
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=70129b87-bd4e-49bb-aa09-77644da73503'

        # Can only get 100 entries at a time. Keep querying until there are
        # no more entries.
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['result']
        repo['hek_kquirk.bpd_employee_earnings'].insert_many(r['records'])
        while len(r['records']) > 0 and 'next' in r['_links']:
            url = 'https://data.boston.gov' + r['_links']['next']
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)['result']
            if len(r['records']) > 0:
                repo['hek_kquirk.bpd_employee_earnings'].insert_many(r['records'])
            
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
