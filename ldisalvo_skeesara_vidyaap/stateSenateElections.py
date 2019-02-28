"""
CS506 : stateSenateElections
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
from requests import get
from requests.exceptions import RequestException
from contextlib import closing


class stateSenateElections(dml.Algorithm):
    contributor = 'ldisalvo_skeesara_vidyaap'
    reads = []
    writes = ['ldisalvo_skeesara_vidyaap.stateSenateElections']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ldisalvo_skeesara_vidyaap', 'ldisalvo_skeesara_vidyaap')

        raw_html = stateSenateElections.simple_get('http://electionstats.state.ma.us/elections/search/year_from:2000/year_to:2018/office_id:9/stage:General')
        html = BeautifulSoup(raw_html, 'html.parser')
        electionsList = html.findAll("tr", {"class": "election_item"})

        electionsRows = []

        for election in electionsList:
            electionData = {}
            electionData['_id'] = election['id'].split('-')[-1]
            electionData['year'] = int(election.findAll("td", {"class": "year"})[0].contents[0])
            electionData['district'] = election.findAll("td", {"class": ""})[1].contents[0]

            electionData['candidates'] = []
            table = election.find("table", {"class": "candidates"})

            winner = table.find("tr", {"class": "is_winner"}).find("td", {"class": "candidate"})
            electionData['candidates'].append(stateSenateElections.buildCandidateRow(winner, True))

            otherCandidates = table.findAll("td", {"class": "candidate"})[1:]
            [electionData['candidates'].append(stateSenateElections.buildCandidateRow(candidate, False)) for candidate in otherCandidates]
            electionData['candidates'] = [x for x in electionData['candidates'] if x is not None]

            electionsRows.append(electionData)

        repo.dropCollection("stateSenateElections")
        repo.createCollection("stateSenateElections")
        repo['ldisalvo_skeesara_vidyaap.stateSenateElections'].insert_many(electionsRows)
        repo['ldisalvo_skeesara_vidyaap.stateSenateElections'].metadata({'complete': True})
        print(repo['ldisalvo_skeesara_vidyaap.stateSenateElections'].metadata())

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


    def buildCandidateRow(candidateDiv, isWinner):
        if not candidateDiv.find("div"):
            return

        candidateData = {}
        candidateData['name'] = candidateDiv.find("div", {"class": "name"}).find("a").contents[0]
        candidateData['party'] = candidateDiv.find("div", {"class": "party"}).contents[0] \
            if candidateDiv.find("div", {"class": "party"}).contents \
            else ''
        candidateData['isWinner'] = isWinner
        return candidateData

    def simple_get(url):
        """
        Attempts to get the content at `url` by making an HTTP GET request.
        If the content-type of response is some kind of HTML/XML, return the
        text content, otherwise return None.
        """
        try:
            with closing(get(url, stream=True)) as resp:
                if stateSenateElections.is_good_response(resp):
                    return resp.content
                else:
                    return None

        except RequestException as e:
            stateSenateElections.log_error('Error during requests to {0} : {1}'.format(url, str(e)))
            return None

    def is_good_response(resp):
        """
        Returns True if the response seems to be HTML, False otherwise.
        """
        content_type = resp.headers['Content-Type'].lower()
        return (resp.status_code == 200
                and content_type is not None
                and content_type.find('html') > -1)

    def log_error(e):
        """
        It is always a good idea to log errors.
        This function just prints them, but you can
        make it do anything.
        """
        print(e)


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof