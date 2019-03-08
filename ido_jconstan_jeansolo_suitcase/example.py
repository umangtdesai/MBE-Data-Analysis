import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class example(dml.Algorithm):
    contributor = 'ido_jconstan_jeansolo_suitcase'
    reads = []
    writes = ['ido_jconstan_jeansolo_suitcase.bu_transportation_study',
              'ido_jconstan_jeansolo_suitcase.property_data',
              'ido_jconstan_jeansolo_suitcase.boston_street_segments']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')


        # OBTAINING FIRST DATASET [Bu Transportation Study]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/bu_transportation_study.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("bu_transportation_study")
        repo.createCollection("bu_transportation_study")
        repo['ido_jconstan_jeansolo_suitcase.bu_transportation_study'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.bu_transportation_study'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.bu_transportation_study'].metadata())

        # OBTAINING SECOND DATA SET [Spark Property Data]
        url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/property_data.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("property_data")
        repo.createCollection("property_data")
        repo['ido_jconstan_jeansolo_suitcase.property_data'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.property_data'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.property_data'].metadata())
    

        # OBTAINING THIRD DATA SET [Boston Street Segments]
        #url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.geojson'
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        #r = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        #repo.dropCollection("boston_street_segments")
        #repo.createCollection("boston_street_segments") 
        #repo['ido_jconstan_jeansolo_suitcase.boston_street_segments'].insert_many(r)
        #repo['ido_jconstan_jeansolo_suitcase.boston_street_segments'].metadata({'complete':True})
        #print(repo['ido_jconstan_jeansolo_suitcase.boston_street_segments'].metadata())   


        # OBTAINING FOURTH DATA SET [Boston work zones]
        url = 'https://drive.google.com/uc?export=download&id=1LhG0cxZgHCU2fqDNaGLdQeBZo9z7gJTj'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
		#response = urllib.request.urlopen(url).read().decode("utf-8")
        #data = pd.read_csv(response, error_bad_lines=False)
        #r = json.loads(data.to_json(orient='records'))
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("zones")
        repo.createCollection("zones")
        repo['ido_jconstan_jeansolo_suitcase.zones'].insert_many(r)
        repo['ido_jconstan_jeansolo_suitcase.zones'].metadata({'complete':True})
        print(repo['ido_jconstan_jeansolo_suitcase.zones'].metadata())


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
        repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        # CHANGES ONLY MADE BELOW THIS COMMENT

        this_script = doc.agent('alg:ido_jconstan_jeansolo_suitcase#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        


        resource_transportStudy = doc.entity('dmi:bu_transportation_study', {'prov:label':'BU Transportation Study', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_propertyData = doc.entity('dmi:property_data', {'prov:label':'Property Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_streetSegments = doc.entity('dbg:cfd1740c2e4b49389f47a9ce2dd236cc_8', {'prov:label':'Boston Street Segments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})

        get_bu_transport_study = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_property_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_boston_street_segments = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_bu_transport_study, this_script)
        doc.wasAssociatedWith(get_property_data, this_script)
        doc.wasAssociatedWith(get_boston_street_segments, this_script)

        doc.usage(get_bu_transport_study, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(get_property_data, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        doc.usage(get_boston_street_segments, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        bu_transportation_study = doc.entity('dat:ido_jconstan_jeansolo_suitcase#bu_transportation_study', {prov.model.PROV_LABEL:'BU Transportation Study', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bu_transportation_study, this_script)
        doc.wasGeneratedBy(bu_transportation_study, get_bu_transport_study, endTime)
        doc.wasDerivedFrom(bu_transportation_study, resource_transportStudy, get_bu_transport_study, get_bu_transport_study, get_bu_transport_study)

        property_data = doc.entity('dat:ido_jconstan_jeansolo_suitcase#property_data', {prov.model.PROV_LABEL:'Property Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property_data, this_script)
        doc.wasGeneratedBy(property_data, get_property_data, endTime)
        doc.wasDerivedFrom(property_data, resource_propertyData, get_property_data, get_property_data, get_property_data)

        boston_street_segments = doc.entity('dat:ido_jconstan_jeansolo_suitcase#bu_transportation_study', {prov.model.PROV_LABEL:'BU Transportation Study', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(boston_street_segments, this_script)
        doc.wasGeneratedBy(boston_street_segments, get_boston_street_segments, endTime)
        doc.wasDerivedFrom(boston_street_segments, resource_streetSegments, get_boston_street_segments, get_boston_street_segments, get_boston_street_segments)


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