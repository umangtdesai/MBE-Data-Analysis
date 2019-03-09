import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import datetime

############################################
# create_travelstats.py
# Script for transforming CTA Station Satistics
############################################

class grab_ctastations(dml.Algorithm):
    contributor = 'smithnj'
    reads = []
    writes = ['smithnj.ctastats']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # ---[ Connect to Database ]---------------------------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('smithnj', 'smithnj')
        repo_name = 'smithnj.travelstats'
        # ---[ Grab Data ]-------------------------------------------
        df = pd.read_csv('/Users/nathaniel/Desktop/smithnj/CTA_Ridership_Totals.csv')  # TODO CHANGE TO WEB LINK
        # ---[ Begin Transformation ]--------------------------------
        df['date'] = pd.to_datetime(df['date'])  # Convert "date" index to datetime
        df.index = df['date']  # set dataframe index to date
        monthly_sums = df.groupby([df.index.month, df['station_id'], df['stationname']]).sum()  # find total num of travelers per station per month
        monthly_sums['rides'] = monthly_sums['rides'].apply(lambda x: x / 4)  # divide rides by four as data is over a four year period
        # ---[ Write Data to JSON ]----------------------------------
        monthly_sums = monthly_sums.reset_index()
        df_json = monthly_sums.to_json(orient='records')
        loaded = json.loads(df_json)
        # ---[ MongoDB Insertion ]-------------------------------------------
        repo.dropCollection('travelstats')
        repo.createCollection('travelstats')
        print('done')
        repo[repo_name].insert_many(loaded)
        repo[repo_name].metadata({'complete': True})
        # ---[ Finishing Up ]-------------------------------------------
        print(repo[repo_name].metadata())
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
#TODO COMPLETE TRAVELSATS PROV
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofchicago.org/Transportation/CTA-Ridership-L-Station-Entries-Daily-Totals/5neh-572f')
        doc.add_namespace('dmc', 'http://datamechanics.io/data/smithnj')
        this_script = doc.agent('alg:smithnj#grab_stationstats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dmc:5neh-572f.json', {'prov:label':'CTA - Ridership - L Station Entries - Daily Totals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        grab_stationstats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(grab_stationstats, this_script)
        doc.usage(grab_stationstats, resource, startTime, None, {prov.model.PROV_TYPE.'ont:Retrieval'})
        stationstats = doc.entity('dat:smithnj#stationstats', {prov.model.PROV_LABEL:''})

        return doc
