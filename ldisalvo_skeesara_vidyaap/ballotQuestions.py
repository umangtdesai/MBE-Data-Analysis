"""
CS504 : ballot_questions
Team : Vidya Akavoor, Lauren DiSalvo, Sreeja Keesara
Description :

Notes : 

February 27, 2019
"""

import dml
import prov.model
import datetime
import uuid
from bs4 import BeautifulSoup
from ldisalvo_skeesara_vidyaap.helper.scraper import scraper
from ldisalvo_skeesara_vidyaap.helper.constants import TEAM_NAME, BALLOT_QUESTIONS

class ballotQuestions(dml.Algorithm):
    contributor = TEAM_NAME
    reads = []
    writes = [BALLOT_QUESTIONS]

    @staticmethod
    def execute(trial=False):
        """
            Scrape electionstats to get data about each MA ballot question (2000-2018)
            and insert into collection
            ex) {
                    "_id" : "7322",
                    "year" : 2018,
                    "number" : "4",
                    "description" : "SUMMARY Sections 3 to 7 of Chapter 44B of the General Laws
                        of Massachusetts, also known as the Community Preservation Act (the 'Act'),
                        establishes a dedicated funding source for the: acquisition, creation and
                        preservation of open space; acquisition, preservation, rehabilitation and
                        restoration of hi...",
                    "type" : "Local Question",
                    "location" : "Various cities/towns"
                }
        """
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(TEAM_NAME, TEAM_NAME)

        # Get HTML of page and pull all ballot question divs
        raw_html = scraper.simple_get('http://electionstats.state.ma.us/ballot_questions/search/year_from:2000/year_to:2018')
        html = BeautifulSoup(raw_html, 'html.parser')
        questionList = html.findAll("tr", {"class": "election_item"})

        ballotQuestionsRows = []

        # Build row for each ballot questions
        for question in questionList:
            questionData = {}
            questionData['_id'] = question['id'].split('-')[-1]
            questionData['year'] = int(question.findAll("td", {"class": "year"})[0].contents[0])
            questionData['number'] = question.findAll("td", {"class": "number"})[0].contents[0]
            questionData['description'] = question.findAll("td", {"class": "display_question"})[0].contents[0]
            questionData['type'] = question.findAll("td", {"class": "bq_types"})[0].contents[0] \
                if question.findAll("td", {"class": "bq_types"})[0].contents \
                else ''
            questionData['location'] = question.findAll("td", {"class": "bq_location"})[0].contents[0]
            ballotQuestionsRows.append(questionData)

        # Insert rows into collection
        repo.dropCollection("ballotQuestions")
        repo.createCollection("ballotQuestions")
        repo[BALLOT_QUESTIONS].insert_many(ballotQuestionsRows)
        repo[BALLOT_QUESTIONS].metadata({'complete': True})
        print(repo[BALLOT_QUESTIONS].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

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