import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class non_poc_early_voting(dml.Algorithm):
    contributor = 'carlosp_jpva_tkay_yllescas'
    reads = ['carlosp_jpva_tkay_yllescas.early_voting', 'carlosp_jpva_tkay_yllescas.demographics_towns']
    writes = ['carlosp_jpva_tkay_yllescas.non_poc_early_voting']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (without API).'''
        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('carlosp_jpva_tkay_yllescas', 'carlosp_jpva_tkay_yllescas')

        repo.dropCollection("non_poc_early_voting")
        repo.createCollection("non_poc_early_voting")

        early_voting = (repo['carlosp_jpva_tkay_yllescas.early_voting']).find()
        town_dem = repo['carlosp_jpva_tkay_yllescas.demographics_towns']
        non_poc_early_voting = {}
        for voting in early_voting:
            for demo in town_dem.find():
                if (voting["City/Town"] != "Grand Total") and (demo["Community"].upper() in voting["City/Town"]) :
                    voting["White"] = float(demo["White"].replace(',',''))/float(demo["Population 2010"].replace(',', ''))
            if (voting["City/Town"] != "Grand Total"):
                non_poc_early_voting[voting["City/Town"]] = (voting["Percentage of Early Voters"], voting["White"])


        final_data = [{x:{"Percentage of Early Voters": non_poc_early_voting[x][0], "Non-POC Voters": round((non_poc_early_voting[x][1]*100), 2)}} for x in non_poc_early_voting]
        repo['carlosp_jpva_tkay_yllescas.non_poc_early_voting'].insert_many(final_data)
        repo['carlosp_jpva_tkay_yllescas.non_poc_early_voting'].metadata({'complete': True})
        print(repo['carlosp_jpva_tkay_yllescas.non_poc_early_voting'].metadata())

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
        repo.authenticate('carlosp_jpva_tkay_yllescas', 'carlosp_jpva_tkay_yllescas')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:carlosp_jpva_tkay_yllescas#non_poc_early_voting',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_registered = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_registered, this_script)
        doc.usage(get_registered, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Registered&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        registered = doc.entity('dat:carlosp_jpva_tkay_yllescas#non_poc_early_voting',
                                {prov.model.PROV_LABEL: 'Registered Voters', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(registered, this_script)
        doc.wasGeneratedBy(registered, get_registered, endTime)
        doc.wasDerivedFrom(registered, resource, get_registered, get_registered, get_registered)

        repo.logout()

        return doc

#non_poc_early_voting.execute()
'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof