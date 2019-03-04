import urllib.request
import json
import csv
import dml
import prov.model
import datetime
import uuid
import pandas as pd 

class Airbnb(dml.Algorithm):
    contributor = 'kelly_colleen'
    reads = []
    writes = ['kelly_colleen.airbnb']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kelly_colleen', 'kelly_colleen')

        #read in csv file
        url = 'http://datamechanics.io/data/airbnb_listing.csv'
        data = pd.read_csv(url, header=0)
        #select relevant columns
        data_airbnb = data[['name','host_id', 'host_name', 'neighbourhood', 'latitude',  'longitude',	'room_type',  
                          'price', 'minimum_nights', 'number_of_reviews',	'availability_365']].copy()
        #change column to appropriate names
        data_airbnb.rename(columns={'name': 'Name', 'neighbourhood': 'Neighborhood', 
                                    'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)
        
        #sort by neighborhood
        data_airbnb.sort_values(by=['Neighborhood'], inplace = True)

        r = json.loads(data_airbnb.to_json(orient='records'))
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("airbnb")
        repo.createCollection("airbnb")
        repo['kelly_colleen.airbnb'].insert_many(r)
        repo['kelly_colleen.airbnb'].metadata({'complete':True})
        print(repo['kelly_colleen.airbnb'].metadata())
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
        repo.authenticate('kelly_colleen', 'kelly_colleen')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        #additional resource
        doc.add_namespace('airbnb', 'http://datamechanics.io/data/airbnb_listing.csv')

        this_script = doc.agent('alg:kelly_colleen#airbnb', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:airbnb', {'prov:label':'Airbnb, Location Search', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_place = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_place, this_script)
        doc.usage(get_place, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        airbnb = doc.entity('dat:kelly_colleen#airbnb', {prov.model.PROV_LABEL:'Place Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(airbnb, this_script)
        doc.wasGeneratedBy(airbnb, get_place, endTime)
        doc.wasDerivedFrom(airbnb, resource, get_place, get_place, get_place)

        repo.logout()
                  
        return doc

# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
Airbnb.execute()
doc = Airbnb.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof