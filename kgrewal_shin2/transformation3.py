
import urllib.request
import requests
import json
import dml
import prov.model
import datetime
import uuid
from kgrewal_shin2 import transformations

class transformation1():
    contributor = 'kgrewal_shin2'
    reads = ['kgrewal_shin2.ubers']
    writes = ['kgrewal_shin2.uber_street_sum']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

        ubers = repo.kgrewal_shin2.ubers.find()

        uber_dests = []
        for l in ubers:
            destination = l['Destination Display Name']
            if destination.endswith('Boston'):
                destination = destination.split(", ")[1]
                uber_dests.append((destination, 1))

        uber_dest_counts = transformations.aggregate(uber_dests, sum)
        neighborhood_ubers = []
        for uber in uber_dest_counts:
            neighborhood_ubers.append({'Neighborhood': uber[0], 'Count': uber[1]})

        repo.dropCollection("neighborhood_ubers")
        repo.createCollection("neighborhood_ubers")
        repo['kgrewal_shin2.neighborhood_ubers'].insert_many(neighborhood_ubers)
        repo['kgrewal_shin2.neighborhood_ubers'].metadata({'complete': True})
        print(repo['kgrewal_shin2.neighborhood_ubers'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:kgrewal_shin2#street_name_prov',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_streets = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_streets, this_script)
        doc.usage(get_streets, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Streets+Without+Landmarks&$select=full_name,gender,zipcode,street_name'
                   }
                  )

        streets = doc.entity('dat:kgrewal_shin2#streets',
                          {prov.model.PROV_LABEL: 'Streets Without Landmarks', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(streets, this_script)
        doc.wasGeneratedBy(streets, get_streets, endTime)
        doc.wasDerivedFrom(streets, resource, get_streets, get_streets, get_streets)

        repo.logout()

        return doc


transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

