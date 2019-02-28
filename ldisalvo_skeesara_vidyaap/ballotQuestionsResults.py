"""
CS504 : ballot_question
Team : Vidya Akavoor, Lauren DiSalvo, Sreeja Keesara
Description :

Notes :

February 26, 2019
"""

import csv
import datetime
import io
import json
import uuid

import dml
import prov.model
import urllib.request

from ldisalvo_skeesara_vidyaap.helper.constants import TEAM_NAME, BALLOT_QUESTIONS, BALLOT_QUESTIONS_RESULTS


class ballotQuestionsResults(dml.Algorithm):
    contributor = TEAM_NAME
    reads = [BALLOT_QUESTIONS]
    writes = [BALLOT_QUESTIONS_RESULTS]

    @staticmethod
    def execute(trial=False):
        """
            Retrieve ballot question results data from electionstats and insert into collection
            ex) {
                    "Locality" : "Bourne",
                    "Ward" : "-",
                    "Pct" : "7",
                    "Yes" : 375,
                    "No" : 914,
                    "Blanks" : 26,
                    "Total Votes Cast" : 1315,
                    "Question ID" : "7303"
                }
        """

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(TEAM_NAME, TEAM_NAME)

        # Get list of question ids from collection
        questionsIds = list(repo[BALLOT_QUESTIONS].find({}, {"_id":1}))
        questionResultsRows = []

        # Use question ids to retrieve data from electionstats for each ballot question
        for question in questionsIds:
            id = question['_id']
            url = 'http://electionstats.state.ma.us/ballot_questions/download/{id}/precincts_include:1/'.format(id=id)
            csvString = urllib.request.urlopen(url).read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(csvString))
            data = json.loads(json.dumps(list(reader)))
            data = [ballotQuestionsResults.cleanData(row, id) for row in data]
            questionResultsRows.extend(data)

        # Insert rows into collection
        repo.dropCollection("ballotQuestionsResults")
        repo.createCollection("ballotQuestionsResults")
        repo[BALLOT_QUESTIONS_RESULTS].insert_many(questionResultsRows)
        repo[BALLOT_QUESTIONS_RESULTS].metadata({'complete': True})
        print(repo[BALLOT_QUESTIONS_RESULTS].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def cleanData(precinctDictionary, id):
        """ Add Question ID field, change num votes values to int"""

        # Add ID field
        precinctDictionary['Question ID'] = id

        # Convert Yes, No, Blanks, Total Votes Values to integers
        precinctDictionary['Yes'] = int(precinctDictionary['Yes'].replace(',',''))
        precinctDictionary['No'] = int(precinctDictionary['No'].replace(',',''))
        precinctDictionary['Blanks'] = int(precinctDictionary['Blanks'].replace(',',''))
        precinctDictionary['Total Votes Cast'] = int(precinctDictionary['Total Votes Cast'].replace(',',''))

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