import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

class averagePerDepartment(dml.Algorithm):
    contributor = 'jkmoy_mfflynn'
    reads = ['jkmoy_mfflynn.fire', 'jkmoy_mfflynnfire.fire_departments']
    writes = ['jkmoy_mfflynn.averagePerDepartment']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')

        # transformation here
        collection = list(repo['jkmoy_mfflynn.fire'].find())
        collection2 = list(repo['jkmoy_mfflynn.fire_departments'].find())
        
        data = [{'Neighborhood':doc['Neighborhood'].strip()} for doc in collection if doc['Neighborhood'] != '']
        s = []
        for x in data:
            x = (x['Neighborhood'], 1)
            s.append(x)

        a = aggregate(s, sum) # List of tuples with neighborhoods and amount of incidents
            
        neighborhoodCounts = [{'Neighborhood': tup[0], 'Count': tup[1]} for tup in a] # List of dicts with the neighborhood and amount of incidents

        #print(collection2[0])
        #print(collection2[1])

        data = [doc['attributes']["PD"] for doc in collection2 if doc['attributes']["PD"].strip() != '']
        for x in data:
            print(x)
        
        i = 0
        for x in range(len(collection2)):
            if i < 0:
                print(collection2[x]['attributes'])
                i += 1
        

        '''
        repo.dropCollection('jkmoy_mfflynn.averagePerDepartment')
        repo.createCollection('jkmoy_mfflynn.averagePerDepartment')
        
        repo['jkmoy_mfflynn.averagePerDepartment'].insert_many(data)
        repo['jkmoy_mfflynn.averagePerDepartment'].metadata({'complete': True})

        print(repo['jkmoy_mfflynn.averagePerDepartment'].metadata())
        '''                           
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

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jkmoy_mfflynn', 'jkmoy_mfflynn')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('fire', 'https://data.boston.gov/datastore/odata3.0/220a4ce5-a991-4336-a19b-159881d7c2e7?$format=json')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
averagePerDepartment.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
averagePerDepartment.execute()


def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]


## eof
