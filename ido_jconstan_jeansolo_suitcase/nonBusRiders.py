import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pymongo
from bson.objectid import ObjectId

class nonBusRiders(dml.Algorithm):
    contributor = 'ido_jconstan_jeansolo_suitcase'
    reads = ['ido_jconstan_jeansolo_suitcase.bu_transportation_study',
             'ido_jconstan_jeansolo_suitcase.property_data']
    writes = ['ido_jconstan_jeansolo_suitcase.PropertyValueNonRiders']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')

        # create new things
        repo.dropCollection("PropertyValueNonRiders")
        repo.createCollection("PropertyValueNonRiders")

        r = repo.ido_jconstan_jeansolo_suitcase.bu_transportation_study.find()

        for i in r:
            i['Address'] = i['Address'].upper()

            if "STREET" in i['Address']:
                 i['Address'] = i['Address'].replace("STREET", "ST")
            if "PLACE" in i['Address']:
                 i['Address'] = i['Address'].replace("PLACE", "PL")
            if "TERRACE" in i['Address']:
                 i['Address'] = i['Address'].replace("TERRACE", "TER")
            if "AVENUE" in i['Address']:
                 i['Address'] = i['Address'].replace("AVENUE", "AVE")
            if "CIRCLE" in i['Address']:
                 i['Address'] = i['Address'].replace("CIRCLE", "CIR")
            if "COURT" in i['Address']:
                 i['Address'] = i['Address'].replace("COURT", "CT")
            if "LANE" in i['Address']:
                 i['Address'] = i['Address'].replace("LANE", "LN")
            if "ROAD" in i['Address']:
                 i['Address'] = i['Address'].replace("ROAD", "RD")
            if "PARK" in i['Address']:
                 i['Address'] = i['Address'].replace("PARK", "PK")

        r1 = repo.ido_jconstan_jeansolo_suitcase.property_data.find()
        for i in r1:
            i['Address 1'] = i['Address 1'].upper()

            if "STREET" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("STREET", "ST")
            if "PLACE" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("PLACE", "PL")
            if "TERRACE" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("TERRACE", "TER")
            if "AVENUE" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("AVENUE", "AVE")
            if "CIRCLE" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("CIRCLE", "CIR")
            if "COURT" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("COURT", "CT")
            if "LANE" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("LANE", "LN")
            if "ROAD" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("ROAD", "RD")
            if "PARK" in i['Address 1']:
                 i['Address 1'] = i['Address 1'].replace("PARK", "PK")

        # Transform 1
        # Get the students who do NOT ride the bus
        notBusRiders = []
        tempFlag = False
        for x in r1:
                print(x)
                for y in r:
                     # if the student does ride the bus
                     if x['Address 1'] in y['Address']:
                          tempFlag = True
                if (tempFlag == False):
                     notBusRiders.append(x)
                else:
                 tempFlag = False
    
          # Get the house price of students who do NOT ride the bus
            nbrHouseValue = []
            for x in notBusRiders:
                nbrHouseValue.append({"Address 1": x['Address 1'], "Assessed Total":x['Assessed Total']})
            for x in nbrHouseValue:
                print(x)

        repo['ido_jconstan_jeansolo_suitcase.PropertyValueNonRiders'].insert_many(nbrHouseValue)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

    #nonBusRiders.execute()