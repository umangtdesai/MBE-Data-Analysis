import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getData(dml.Algorithm):
    contributor = 'asadeg02_gxy9598'
    reads = []
    writes = ['asadeg02_gxy9598.building_permits']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')

        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=6ddcd912-32a0-43df-9908-63574f8c7e77&limit=125650'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        r = (r['result']['records'])
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("building_permits")
        repo.createCollection("building_permits")
        repo["asadeg02_gxy9598.building_permits"].insert_many(r)
        repo["asadeg02_gxy9598.building_permits"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.building_permits"].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:asadeg02_gxy9598#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_building_permits = doc.entity('cob:ufcx-3fdn', {'prov:label':'Permits DataBase', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_building_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Building Permits', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_building_permits, this_script)
        doc.usage(get_building_permits, resource_building_permits, startTime)
        building_permits = doc.entity('dat:asadeg02_gxy9598#building_permits', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(building_permits, this_script)
        doc.wasGeneratedBy(building_permits, get_building_permits, endTime)
        doc.wasDerivedFrom(building_permits, resource_building_permits, get_building_permits,get_building_permits,get_building_permits)

        return doc




# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
'''example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

getData.execute()
doc = getData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
## eof
