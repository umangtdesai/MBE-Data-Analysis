import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd


class topCertifiedCompanies(dml.Algorithm):
    contributor = 'ashwini_gdukuray_justini_utdesai'
    reads = ['ashwini_gdukuray_justini_utdesai.topCompanies', 'ashwini_gdukuray_justini_utdesai.masterList']
    writes = ['ashwini_gdukuray_justini_utdesai.topCertCompanies']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')

        masterList = repo['ashwini_gdukuray_justini_utdesai.masterList']
        topCompanies = repo['ashwini_gdukuray_justini_utdesai.topCompanies']

        masterListDF = pd.DataFrame(list(masterList.find()))
        topCompaniesDF = pd.DataFrame(list(topCompanies.find()))

        topCompaniesDF = topCompaniesDF.rename(index=str, columns={'Firm': 'Business Name'})

        # create a more uniform ID
        businessIDs = []
        for index, row in topCompaniesDF.iterrows():
            busName = row['Business Name']
            cleanedText = busName.upper().strip().replace(' ','').replace('.','').replace(',','').replace('-','')
            businessIDs.append(cleanedText)

        topCompaniesDF['B_ID'] = pd.Series(businessIDs, index=topCompaniesDF.index)
        

        merged = pd.merge(masterListDF, topCompaniesDF, how='inner', on=['B_ID'])

        #print(merged['B_ID'])

        #records = json.loads(topCompaniesDF.T.to_json()).values()

        repo.dropCollection("topCertCompanies")
        repo.createCollection("topCertCompanies")
        repo['ashwini_gdukuray_justini_utdesai.topCertCompanies'].insert_many(merged.to_dict('records'))
        repo['ashwini_gdukuray_justini_utdesai.topCertCompanies'].metadata({'complete': True})
        print(repo['ashwini_gdukuray_justini_utdesai.topCertCompanies'].metadata())

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
        pass

        """
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ashwini_gdukuray_justini_utdesai/Top25Companies.csv')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat2', 'http://datamechanics.io/data/ashwini_gdukuray_justini_utdesai/Top25Companies.json')
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ashwini_gdukuray_justini_utdesai#topCompanies',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:ashwini_gdukuray_justini_utdesai#topCompanies',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        resource2 = doc.entity('dat2:ashwini_gdukuray_justini_utdesai#topCompanies',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        act = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(resource2, this_script)
        doc.usage(act, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        doc.wasAttributedTo(resource, this_script)
        doc.wasGeneratedBy(resource2, act, endTime)
        doc.wasDerivedFrom(resource2, resource, act, act, act)


        repo.logout()

        return doc
        """


'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
