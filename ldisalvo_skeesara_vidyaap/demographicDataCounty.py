"""
CS504 : demographicData.py
Team : Vidya Akavoor, Lauren DiSalvo, Sreeja Keesara
Description :

Notes :

February 28, 2019
"""

import csv
import datetime
import io
import json
import uuid
import pandas as pd

import dml
import prov.model
import urllib.request

from ldisalvo_skeesara_vidyaap.helper.constants import TEAM_NAME, COUNTY_URL, MA_COUNTY_LIST


class demographicDataCounty(dml.Algorithm):
    contributor = TEAM_NAME
    reads = []
    writes = ['ldisalvo_skeesara_vidyaap.DEMOGRAPHIC_DATA_COUNTY']

    @staticmethod
    def execute(trial=False):
        """
            Retrieve demographic data by country from census.gov and insert into collection
            #########FIX THIS############## ex) {
                    "City/Town" : "Barnstable",
                    "Ward" : "-",
                    "Pct" : "7",
                    "Election ID" : "131582",
                    "William L Crocker, Jr" : 1079,
                    "Paul J Cusack" : 1059,
                    "All Others" : 5,
                    "Blanks" : 42,
                    "Total Votes Cast" : 2185
                }
        """
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(TEAM_NAME, TEAM_NAME)

        # Retrieve data from census.gov for each county (note: retrieval generates csv with max of 6 locations)
        url = COUNTY_URL
        county_divisions = [MA_COUNTY_LIST[:6], MA_COUNTY_LIST[6:12], MA_COUNTY_LIST[12:14]]
        response = []
        all_dfs= []
        for divisions in county_divisions:
            counties = ""
            for county in divisions:
                counties += (county.lower() + "massachusetts,").replace(" ", "")
            counties = counties[:-1]
            url = COUNTY_URL[:43] + counties + COUNTY_URL[43:]
            df = pd.read_csv(urllib.request.urlopen(url))
            df = df.drop(df.index[-20:])
            df.set_index("Fact", inplace=True)
            df = df.transpose()
            df = df[~df.index.str.contains("Note")]
            cols = list(df)
            all_dfs.append(df)

        for df in all_dfs:
            df.columns = cols

        joined_df = pd.concat(all_dfs)
        df = joined_df.dropna(axis=1, how='all')
        df_to_json = [json.loads((df.to_json(orient='index')))]

        #print(type(df_to_json[0]))

        # Insert rows into collection
        repo.dropCollection("DEMOGRAPHIC_DATA_COUNTY")
        repo.createCollection("DEMOGRAPHIC_DATA_COUNTY")
        repo["ldisalvo_skeesara_vidyaap.DEMOGRAPHIC_DATA_COUNTY"].insert_many(df_to_json)
        repo["ldisalvo_skeesara_vidyaap.DEMOGRAPHIC_DATA_COUNTY"].metadata({'complete': True})
        print(repo["ldisalvo_skeesara_vidyaap.DEMOGRAPHIC_DATA_COUNTY"].metadata())

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

#demographicDataCounty.execute()

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof


