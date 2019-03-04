import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
#from .scrapeBostonGov import getData as getDataByScaping

class getData(dml.Algorithm):
    contributor = 'asadeg02_gxy9598'
    reads = []
    writes = ['asadeg02_gxy9598.building_permits', 'asadeg02_gxy9598.property_details', 'asadeg02_gxy9598.crime_incident_report', 'asadeg02_gxy9598.active_food_stablishment']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asadeg02_gxy9598', 'asadeg02_gxy9598')


        ###################------------get building permit data---------#########################################################        

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
        print('Load Building Permits')

        ##################################################################################

        ''' data = repo["asadeg02_gxy9598.building_permits"].find()
        address_list = [row['ADDRESS'] for row in data]
        print(address_list)
        results = getDataByScaping(address_list[:5])
        repo.dropCollection("asadeg02_gxy9598.property_details")
        repo.createCollection("asadeg02_gxy9598.property_details")
        
        _id = 0
        for r in results:
            r['_id'] = _id
            repo["asadeg02_gxy9598.property_details"].insert(r)
            _id += 1
        repo["asadeg02_gxy9598.property_details"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.property_details"].metadata())
        print('Finish scraping')

        '''

       #################################----------------Get crime incident data --------------###############################################
       
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=12cb3883-56f5-47de-afa5-3b1cf61b257b&limit=366640'
        response = urllib.request.urlopen(url).read().decode("utf-8")
                
        r = json.loads(response)
        r = (r['result']['records'])
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime_incident_report")
        repo.createCollection("crime_incident_report")
        repo["asadeg02_gxy9598.crime_incident_report"].insert_many(r)
        repo["asadeg02_gxy9598.crime_incident_report"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.crime_incident_report"].metadata())
        print('Load crime incident report')

     

       #################################----------------Get active food stablishment data-------------###############################################
       
        url = 'https://data.boston.gov/api/3/action/datastore_search?resource_id=f1e13724-284d-478c-b8bc-ef042aa5b70b&limit=3010'
        response = urllib.request.urlopen(url).read().decode("utf-8")
                
        r = json.loads(response)
        r = (r['result']['records'])
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("active_food_stablishment")
        repo.createCollection("active_food_stablishment")
        repo["asadeg02_gxy9598.active_food_stablishment"].insert_many(r)
        repo["asadeg02_gxy9598.active_food_stablishment"].metadata({'complete':True})
        print(repo["asadeg02_gxy9598.active_food_stablishment"].metadata())
        print('Load active food stablishment')

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

      ###############################################################################################################################################

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
        building_permits = doc.entity('dat:asadeg02_gxy9598#building_permits', {prov.model.PROV_LABEL:'Building Permits Database', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(building_permits, this_script)
        doc.wasGeneratedBy(building_permits, get_building_permits, endTime)
        doc.wasDerivedFrom(building_permits, resource_building_permits, get_building_permits,get_building_permits,get_building_permits)


        resource_crimes_incident = doc.entity('cob:ufcx-3fdn', {'prov:label':'Crimes Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimes_incident = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Crimes Incident Reports', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_crimes_incident, this_script)
        doc.usage(get_crimes_incident, resource_crimes_incident)
        crimes_incident = doc.entity('dat:asadeg02_gxy9598#crime_incident_report', {prov.model.PROV_LABEL:'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes_incident, this_script)
        doc.wasGeneratedBy(crimes_incident, get_crimes_incident, endTime)
        doc.wasDerivedFrom(crimes_incident, resource_crimes_incident, get_crimes_incident, get_crimes_incident, get_crimes_incident)

        resource_active_food_stablishment = doc.entity('cob:ufcx-3fdn', {'prov:label':'Actvie food stablishment', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_active_food_stablishment = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Active Food Stablishment', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_active_food_stablishment, this_script)
        doc.usage(get_active_food_stablishment, resource_active_food_stablishment)
        active_food_stablishment = doc.entity('dat:asadeg02_gxy9598#active_food_stablishment', {prov.model.PROV_LABEL:'Active Food Stablishment', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(active_food_stablishment, this_script)
        doc.wasGeneratedBy(active_food_stablishment, get_active_food_stablishment, endTime)
        doc.wasDerivedFrom(active_food_stablishment, resource_active_food_stablishment, get_active_food_stablishment, get_active_food_stablishment, get_active_food_stablishment)

        repo.logout()
        return doc






getData.execute()
doc = getData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
## eof
