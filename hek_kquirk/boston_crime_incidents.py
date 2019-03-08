import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class boston_crime_incidents(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = []
    writes = ['hek_kquirk.boston_crime_incidents']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("boston_crime_incidents")
        repo.createCollection("boston_crime_incidents")

        # Api call for employee earnings dataset
        url = 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22%20WHERE%20year=%272017%27%20ORDER%20BY%20occurred_on_date'
        
        # Can only get 32000 entries at a time. Keep querying until there are
        # no more entries.
        num_entries = 0
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['result']
        repo['hek_kquirk.boston_crime_incidents'].insert_many(r['records'])
        num_entries += len(r['records'])
        while 'records_truncated' in r:
            url = 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%2212cb3883-56f5-47de-afa5-3b1cf61b257b%22%20WHERE%20year=%272017%27%20ORDER%20BY%20occurred_on_date%20OFFSET%20' + str(num_entries)
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)['result']
            repo['hek_kquirk.boston_crime_incidents'].insert_many(r['records'])
            num_entries += len(r['records'])
            
        repo['hek_kquirk.boston_crime_incidents'].metadata({'complete':True})
        print(repo['hek_kquirk.boston_crime_incidents'].metadata())

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
