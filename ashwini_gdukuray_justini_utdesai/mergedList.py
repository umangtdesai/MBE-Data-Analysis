import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid


class mergedList(dml.Algorithm):
    contributor = 'ashwini_gdukuray_justini_utdesai'
    reads = ['ashwini_gdukuray_justini_utdesai.masterList', 'ashwini_gdukuray_justini_utdesai.nonMBEmasterList']
    writes = ['ashwini_gdukuray_justini_utdesai.mergedList']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')

        MBElist = repo['ashwini_gdukuray_justini_utdesai.masterList']
        NMBElist = repo['ashwini_gdukuray_justini_utdesai.nonMBEmasterList']

        # Trial mode implementation
        if (trial):
            MBElistDF = pd.DataFrame(list(MBElist.find()))[:100]
            NMBElistDF = pd.DataFrame(list(NMBElist.find()))[:100]
        else:
            MBElistDF = pd.DataFrame(list(MBElist.find()))
            NMBElistDF = pd.DataFrame(list(NMBElist.find()))

        # Add back a column for MBE status
        MBElistDF['MBE Status'] = 'Y'
        NMBElistDF['MBE Status'] = 'N'

        # Merge two lists
        combinedDF = pd.concat([MBElistDF, NMBElistDF])

        # Standardize industry column
        categories = {
                        'Finance': ['financial', 'finance', 'accounting', 'bank', 'money', 'equity', 'asset', 'payroll', 'economic', 'stock', 'fine art', 'novelties'],
                        'Technology': ['information', 'technology', 'tech', 'computer', 'software', 'data', 'it professional'],
                        'Architecture': ['architecture', 'architectural', 'engineering', 'architect',  'interior design'],
                        'Carpentry': ['carpentry', 'wood', 'drywall'],
                        'HVAC': ['hvac', 'air conditioning', 'heating', 'insulation'],
                        'Landscaping': ['landscaping', 'landscape', 'snow'],
                        'Janitorial Services': ['janitorial'],
                        'Music': ['music', 'acoustic', 'guitar'],
                        'Answering Services': ['answering', 'telephone', 'telecommunications'],
                        'Home': ['cabinet', 'appliance', 'kitchen', 'bath', 'door', 'home', 'window', 'remodel', 'flag'],
                        'Hospitality': ['catering', 'wedding', 'party', 'event planner', 'cater', 'planning', 'hospitality'],
                        'Construction': ['concrete', 'construction', 'operat', 'glass', 'scaffold'],
                        'Environment': ['environment', 'sustainability', 'sustainable'],
                        'Electrical': ['electric', 'electronic'],
                        'Marketing/Advertising': ['marketing', 'advertising', 'leadership', 'strategic planning', 'relations', 'community', 'brand', 'presentation'],
                        'Maintenance/Repairs': ['elevator', 'hardware', 'maintenance'],
                        'Pest Control': ['pest', 'exterminator', 'exterminat'],
                        'Fencing': ['fence', 'fencing', 'barrier'],
                        'Flooring/Roofing': ['flooring', 'floor', 'carpet', 'roofing', 'tile'],
                        'Fuel': ['oil', 'gasoline', 'gas', 'energy'],
                        'Contractor': ['contract'],
                        'Legal': ['legal', 'lawyer'],
                        'Cleanup': ['lead abatement', 'asbestos', 'demolition', 'trash', 'garbage', 'cleaning', 'compost'],
                        'Management/Real Estate': ['management', 'leadership', 'project cont', 'real estate'],
                        'Masonry': ['masonry', 'stone'],
                        'Metals': ['metal', 'steel', 'iron', 'gold', 'jewell'],
                        'Office Supplies': ['office', 'stationery'],
                        'Painting': ['paint'],
                        'Media': ['photo', 'video', 'media', 'printing', 'graphic', 'news', 'logo', 'arts'],
                        'Plumbing': ['plumbing', 'plumber'],
                        'Insurance': ['insurance', 'claims'],
                        'Security/Safety': ['security', 'watch guard', 'safety', 'fire'],
                        'Site Improvements': ['road', 'paving', 'sidewalk', 'site util', 'sign procure'],
                        'Transportation': ['bus', 'car', 'taxi', 'transport', 'airplane', 'travel', 'delivery', 'livery', 'trucking', 'moving', 'shuttle'],
                        'Services': ['translation', 'resident', 'language', 'supplier'],
                        'Floral': ['flower', 'floral'],
                        'Fitness/Health': ['fitness', 'health', 'recreational', 'medical', 'yoga', 'barre'],
                        'Food': ['food', 'pantry', 'beverage', 'produce'],
                        'Municipality': ['poverty', 'permit', 'parking', 'state', 'court'],
                        'Counseling': ['counseling', 'therapy'],
                        'Research': ['research', 'testing', 'laboratory'],
                        'Non Profit/Community': ['community', 'housing', 'treatment', 'shelter', 'mental', 'domestic', 'habitat'],
                        'Consumer Services': ['hair', 'salon', 'nail'],
                        'Automobile': ['vehicle', 'automobile', 'body shop', 'auto damage'],
                        'Apparel': ['clothes', 'uniform', 'apparel'],
                        'Employment': ['employment', 'recruit', 'interview', 'skills', 'employee', 'workforce', 'training', 'staffing'],
                        'Education': ['education', 'school', 'children', 'college']
                     }

        combinedDF['IndustryID'] = 'Temp'

        for index, row in combinedDF.iterrows():
            industry = row['Industry'].lower()

            for key in categories:
                for cat in categories[key]:
                    if (cat in industry):
                        row['IndustryID'] = key
                        break
                # check if company was sorted into a category already
                if (row['IndustryID'] != 'Temp'):
                    break

        combinedDF = combinedDF.reset_index(drop=True)
        combinedDF = combinedDF.drop(columns=['Industry'])

        #records = json.loads(industryDF.T.to_json()).values()

        repo.dropCollection("mergedList")
        repo.createCollection("mergedList")
        repo['ashwini_gdukuray_justini_utdesai.mergedList'].insert_many(combinedDF.to_dict('records'))
        repo['ashwini_gdukuray_justini_utdesai.mergedList'].metadata({'complete': True})
        print(repo['ashwini_gdukuray_justini_utdesai.mergedList'].metadata())

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

        this_script = doc.agent('alg:ashwini_gdukuray_justini_utdesai#mergedList()',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        masterList = doc.entity('dat:ashwini_gdukuray_justini_utdesai#masterList',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        nonMBEmasterList = doc.entity('dat:ashwini_gdukuray_justini_utdesai#nonMBEmasterList',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        mergedList = doc.entity('dat:ashwini_gdukuray_justini_utdesai#mergedList',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        act = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(act, this_script)
        doc.usage(act, masterList, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(act, nonMBEmasterList, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.wasAttributedTo(mergedList, this_script)
        doc.wasGeneratedBy(mergedList, act, endTime)
        doc.wasDerivedFrom(mergedList, masterList, act, act, act)
        doc.wasDerivedFrom(mergedList, nonMBEmasterList, act, act, act)

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


