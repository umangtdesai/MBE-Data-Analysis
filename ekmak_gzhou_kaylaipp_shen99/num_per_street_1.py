import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import zillow
import requests
import xmltodict
import csv

#number of buildings per street in south boston 
class num_per_street_1(dml.Algorithm):
    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = ['ekmak_gzhou_kaylaipp_shen99.accessing_data', 'ekmak_gzhou_kaylaipp_shen99.address_data']
    writes = ['ekmak_gzhou_kaylaipp_shen99.num_per_street_1']

    @staticmethod 
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        #list of tuples in format (street name, street num, 1) aka (beacon st  , 362, 1)                  
        #contains all street names for accessing & address data = union 
        all_streets = []
        #retreive boston address data 
        address_data = repo.ekmak_gzhou_kaylaipp_shen99.address_data.find()
        for info in address_data: 
            zipcode = info['ZIP_CODE']
            st_full = info['FULL_STREET_NAME'].lower().replace(".", "")  #beacon st
            st_num = info['STREET_NUMBER_SORT']                          #362

            if zipcode == '02127': 
                #get list of tuples that have same street name 
                same_street = list(filter(lambda t: t[0] == st_full, all_streets))
                #get list of any duplicate buildings
                same_building = list(filter(lambda t: t[1] == st_num, same_street))
                #don't add any duplicate buildings 
                if len(same_building) > 0: 
                    continue
                all_streets.append((st_full, st_num, 1))

        #retrieve accessing data
        accessing_data = repo.ekmak_gzhou_kaylaipp_shen99.accessing_data.find()
        for info in accessing_data: 
            zipcode = info['ZIPCODE']
            st_name = info['ST_NAME'].lower().replace(".", "")         #beacon
            st_num = info['ST_NUM']                                    #362
            st_suffix = info['ST_NAME_SUF'].lower().replace(".", "")   #st
            st_full = st_name + ' ' + st_suffix
            if zipcode == '02127': 
                #get list of tuples that have same street name 
                same_street = list(filter(lambda t: t[0] == st_full, all_streets))
                #get list of any duplicate buildings
                same_building = list(filter(lambda t: t[1] == st_num, same_street))
                #don't add any duplicate buildings 
                if len(same_building) > 0: 
                    continue
                all_streets.append((st_full, st_num, 1))


        #take away building numbers t[1], only keep street name and count
        all_streets = project(all_streets, lambda t: (t[0], t[2]))

        #aggregation by key (st name)
        total = reduce(lambda street,count: (street, sum(count)), all_streets)

        #create new database 
        repo.dropCollection("num_per_street_1")
        repo.createCollection("num_per_street_1")

        #add to database in {street: count} form 
        for t in total: 
            repo['ekmak_gzhou_kaylaipp_shen99.num_per_street_1'].insert_one({t[0]:t[1]})

        print('inserted number of houses per street data')
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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

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

def map(f, R):
    return [t for (k,v,s) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]
def project(R, p):
    return [p(t) for t in R]


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

num_per_street_1.execute()
## eof
