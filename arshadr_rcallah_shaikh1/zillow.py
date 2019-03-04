import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class example(dml.Algorithm):
    contributor = 'arshadr_rcallah_shaikh1'
    reads = []
    #writes = ['arshadr_rcallah_shaikh1.assessor18', 'arshadr_rcallah_shaikh1.permits', 'arshadr_rcallah_shaikh1.incomes']
    writes = ['arshadr_rcallah_shaikh1.rental_prices', 'arshadr_rcallah_shaikh1.home_values', 'arshadr_rcallah_shaikh1.property_values']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')

        # Zillow rent prices
        url = 'http://datamechanics.io/data/arshadr_rcallah_shaikh1/City_ZriPerSqft_AllHomes.json'
        response = urllib.request.urlopen(url).read().decode("utf-8", errors = 'replace')
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("rental_prices")
        repo.createCollection("rental_prices")
        repo['arshadr_rcallah_shaikh1.rental_prices'].insert_many(r)
        repo['arshadr_rcallah_shaikh1.rental_prices'].metadata({'complete':True})
        print(repo['arshadr_rcallah_shaikh1.rental_prices'].metadata())

        # Zillow single family home buying prices
        url = 'http://datamechanics.io/data/arshadr_rcallah_shaikh1/City_Zhvi_SingleFamilyResidence.json'
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("home_values")
        repo.createCollection("home_values")
        repo['arshadr_rcallah_shaikh1.home_values'].insert_many(r)


        # Fiscal year 2019 Chelsea property data (https://www.chelseama.gov/assessor/pages/chelsea-property-data)
        url = "http://datamechanics.io/data/arshadr_rcallah_shaikh1/fy19_chelsea_property_data.json"
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("property_values")
        repo.createCollection("property_values")
        repo['arshadr_rcallah_shaikh1.property_values'].insert_many(r)

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
        repo.authenticate('arshadr_rcallah_shaikh1', 'arshadr_rcallah_shaikh1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:arshadr_rcallah_shaikh1#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_assessor18 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_permits, this_script)
        doc.wasAssociatedWith(get_assessor18, this_script)
        doc.usage(get_permits, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?type=Animal+permits&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_assessor18, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Query':'?type=Animal+assessor18&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        assessor18 = doc.entity('dat:arshadr_rcallah_shaikh1#assessor18', {prov.model.PROV_LABEL:'Animals assessor18', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(assessor18, this_script)
        doc.wasGeneratedBy(assessor18, get_assessor18, endTime)
        doc.wasDerivedFrom(assessor18, resource, get_assessor18, get_assessor18, get_assessor18)

        permits = doc.entity('dat:arshadr_rcallah_shaikh1#permits', {prov.model.PROV_LABEL:'Animals permits', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(permits, this_script)
        doc.wasGeneratedBy(permits, get_permits, endTime)
        doc.wasDerivedFrom(permits, resource, get_permits, get_permits, get_permits)

        incomes = doc.entity('dat:arshadr_rcallah_shaikh1#incomes', {prov.model.PROV_LABEL:'Animals incomes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(incomes, this_script)
        doc.wasGeneratedBy(incomes, get_incomes, endTime)
        doc.wasDerivedFrom(incomes, resource, get_incomes, get_incomes, get_incomes)


        repo.logout()

        return doc


# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
'''
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof