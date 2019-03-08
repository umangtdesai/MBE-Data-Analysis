import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class bpd_fio(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = []
    writes = ['hek_kquirk.bpd_fio']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("bpd_fio")
        repo.createCollection("bpd_fio")

        # Api call for employee earnings dataset
        url = 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2235f3fb8f-4a01-4242-9758-f664e7ead125%22'
        
        # Can only get 32000 entries at a time. Keep querying until there are
        # no more entries.
        num_entries = 0
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['result']
        repo['hek_kquirk.bpd_fio'].insert_many(r['records'])
        num_entries += len(r['records'])
        while 'records_truncated' in r:
            url = 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2235f3fb8f-4a01-4242-9758-f664e7ead125%22%20ORDER%20BY%20contact_date%20OFFSET%20' + str(num_entries)
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)['result']
            repo['hek_kquirk.bpd_fio'].insert_many(r['records'])
            num_entries += len(r['records'])
            
        repo['hek_kquirk.bpd_fio'].metadata({'complete':True})
        print(repo['hek_kquirk.bpd_fio'].metadata())

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
