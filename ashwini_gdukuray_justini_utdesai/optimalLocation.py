import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid


class optimalLocation(dml.Algorithm):
    contributor = 'ashwini_gdukuray_justini_utdesai'
    reads = ['ashwini_gdukuray_justini_utdesai.mergedList']
    writes = ['ashwini_gdukuray_justini_utdesai.optimalLocations']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')

        mergedList = repo['ashwini_gdukuray_justini_utdesai.mergedList']

        if (trial):
            mergedListDF = pd.DataFrame(list(mergedList.find()))[:100]
        else:
            mergedListDF = pd.DataFrame(list(mergedList.find()))

        # Break dataframe into separate dataframes by zip code
        zipCodeDF = mergedListDF.groupby('Zip')
        zipGroups = [zipCodeDF.get_group(x) for x in zipCodeDF.groups]

        optimalLocationsDict = {'Zip': [], 'Number of MBE Additions': []}

        # find the optimal values per zip code
        for grp in zipGroups:
            currentZip = grp.iloc[0]['Zip']

            # generate a list of industries for this zip code (non MBE only)
            # and create a banned industry lists (where MBEs already exist
            industryList = set()
            bannedIndustries = set()
            for index, row in grp.iterrows():
                if (row['MBE Status'] == 'N'):
                    industryList.add(row['IndustryID'])
                else:
                    bannedIndustries.add(row['IndustryID'])

            # count how many MBEs can be added to this zip code
            count = 0
            for index, row in mergedListDF.iterrows():
                ind = row['IndustryID']
                if (row['Zip'] != currentZip and ind in industryList and ind not in bannedIndustries and row['MBE Status'] == 'Y'):
                    # have all the row info as well (business name, industry, location)
                    bannedIndustries.add(ind)
                    count += 1


            optimalLocationsDict['Zip'].append(currentZip)
            optimalLocationsDict['Number of MBE Additions'].append(count)

        optimalLocationsDF = pd.DataFrame(optimalLocationsDict)
        optimalLocationsDF = optimalLocationsDF.sort_values('Number of MBE Additions', ascending=False)
        optimalLocationsDF = optimalLocationsDF.reset_index(drop=True)

        #print(optimalLocationsDF)

        #records = json.loads(industryDF.T.to_json()).values()

        repo.dropCollection("optimalLocations")
        repo.createCollection("optimalLocations")
        repo['ashwini_gdukuray_justini_utdesai.optimalLocations'].insert_many(optimalLocationsDF.to_dict('records'))
        repo['ashwini_gdukuray_justini_utdesai.optimalLocations'].metadata({'complete': True})
        print(repo['ashwini_gdukuray_justini_utdesai.optimalLocations'].metadata())

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
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=ashwini_gdukuray_justini_utdesai/')

        this_script = doc.agent('alg:ashwini_gdukuray_justini_utdesai#optimalLocation()',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        mergedList = doc.entity('dat:ashwini_gdukuray_justini_utdesai#mergedList',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        optimalLocations = doc.entity('dat:ashwini_gdukuray_justini_utdesai#optimalLocations',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        act = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(act, this_script)
        doc.usage(act, mergedList, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        doc.wasAttributedTo(optimalLocations, this_script)
        doc.wasGeneratedBy(optimalLocations, act, endTime)
        doc.wasDerivedFrom(optimalLocations, mergedList, act, act, act)
        
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
