import urllib.request
import requests
import pandas as pd
import json
import dml
import prov.model
import datetime
import uuid
import io
import csv
import fire_incident_urlList


class FireIncidentReport(dml.Algorithm):
    contributor = 'liweixi_mogjzhu'
    reads = []
    writes = ['liweixi_mogujzhu.fire_incident_report']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')

        urls = fire_incident_urlList.getUrlList()
        repo.dropCollection("fire_incident_report")
        repo.createCollection("fire_incident_report")
        for url in urls:
            fire_incident = pd.read_csv(url)
            fire_incident = json.loads(fire_incident.to_json(orient='records'))
            # print(fire_incident[:1])
            # s = json.dumps(r, sort_keys=True, indent=2)
            repo['liweixi_mogujzhu.fire_incident_report'].insert_many(fire_incident)
            repo['liweixi_mogujzhu.fire_incident_report'].metadata({'complete': True})
            print(repo['liweixi_mogujzhu.fire_incident_report'].metadata())

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
        repo.authenticate('liweixi_mogujzhu', 'liweixi_mogujzhu')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/fire-incident-reporting')

        this_script = doc.agent('alg:liweixi_mogujzhu#fire_incident_report',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:Boston Fire Incident Reporting',
                              {'prov:label': '121, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        get_fire_incident_report = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fire_incident_report, this_script)
        doc.usage(get_fire_incident_report, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'
                   }
                  )

        fire_incident_report = doc.entity('dat:liweixi_mogujzhu#fire_incident_report',
                          {prov.model.PROV_LABEL: 'Boston Fire Incident Report', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fire_incident_report, this_script)
        doc.wasGeneratedBy(fire_incident_report, get_fire_incident_report, endTime)
        doc.wasDerivedFrom(fire_incident_report, resource, get_fire_incident_report, get_fire_incident_report, get_fire_incident_report)

        repo.logout()

        return doc


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
'''
FireIncidentReport.execute()
doc = FireIncidentReport.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof