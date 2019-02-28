"""
CS506 : stateHouseElectionsResults
Team : Vidya Akavoor, Lauren DiSalvo, Sreeja Keesara
Description :

Notes : 

February 28, 2019
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io


class stateHouseElectionsResults(dml.Algorithm):
    contributor = 'ldisalvo_skeesara_vidyaap'
    reads = ['ldisalvo_skeesara_vidyaap.stateHouseElections']
    writes = ['ldisalvo_skeesara_vidyaap.stateHouseElectionsResults']

    @staticmethod
    def execute(trial=False):
        '''Retrieve and store all MA ballot question voter data 2000 - 2018'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ldisalvo_skeesara_vidyaap', 'ldisalvo_skeesara_vidyaap')

        #get list of question ids
        electionIds = list(repo['ldisalvo_skeesara_vidyaap.stateHouseElections'].find({}, {"_id":1}))
        electionResultsRows = []

        for question in electionIds:
            id = question['_id']
            url = 'http://electionstats.state.ma.us/elections/download/{id}/precincts_include:1/'.format(id=id)
            csvString = urllib.request.urlopen(url).read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(csvString))
            data = json.loads(json.dumps(list(reader)))
            data = [stateHouseElectionsResults.cleanData(row, id) for row in data[1:]]
            electionResultsRows.extend(data)

        repo.dropCollection("stateHouseElectionsResults")
        repo.createCollection("stateHouseElectionsResults")
        repo['ldisalvo_skeesara_vidyaap.stateHouseElectionsResults'].insert_many(electionResultsRows)
        repo['ldisalvo_skeesara_vidyaap.stateHouseElectionsResults'].metadata({'complete': True})
        print(repo['ldisalvo_skeesara_vidyaap.stateHouseElectionsResults'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def cleanData(precinctDictionary, id):
        '''Add additional fields and convert appropriate values to integers'''

        # Add ID field
        precinctDictionary['Election ID'] = id

        keysToChange = list(precinctDictionary.keys())[3:-1]
        for key in keysToChange:
            precinctDictionary[key] = int(precinctDictionary[key].replace(',',''))
            precinctDictionary[key.replace('.','')] = precinctDictionary.pop(key)

        return precinctDictionary


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg',
                          'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat',
                          'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'],
                                 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label': '311, Service Requests',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})
        get_found = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL: 'Animals Lost',
                                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL: 'Animals Found',
                                                   prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()

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