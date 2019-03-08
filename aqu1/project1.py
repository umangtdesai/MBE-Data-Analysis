import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import education
import income
import mbta_stops
import schools

class project1(dml.Algorithm):
    contributor = 'aqu1'
    reads = []
    writes = ['aqu1.education_data', 'aqu1.income_data', 'aqu1.mbta_stops_data', 'aqu1.schools_data']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aqu1', 'aqu1')

        education_data = education.education()
        print(type(education_data))
        repo.dropCollection("education_data")
        repo.createCollection("education_data")
        repo['aqu1.education_data'].insert_many(education_data)

        income_data = income.income()
        repo.dropCollection("income_data")
        repo.createCollection("income_data")
        repo['aqu1.income_data'].insert_many(income_data)
        
        mbta_stops_data = mbta_stops.mbta_stops()
        repo.dropCollection("mbta_stops_data")
        repo.createCollection("mbta_stops_data")
        repo['aqu1.mbta_stops_data'].insert_many(mbta_stops_data)
        
        schools_data = schools.schools()
        repo.dropCollection("schools_data")
        repo.createCollection("schools_data")
        repo['aqu1.schools_data'].insert_many(schools_data)
        
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
        repo.authenticate('aqu1', 'aqu1')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.boston.gov/dataset/') # Boston Data Portal
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/') # Boston Open Data 
        doc.add_namespace('agh', 'https://opendata.arcgis.com/datasets/') # Arc GIS Hub

        this_script = doc.agent('alg:aqu1#', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Education Attainment Report
        resource_education = doc.entity('bdp:8202abf2-8434-4934-959b-94643c7dac18/resource/bb0f26f8-e472-483c-8f0c-e83048827673/download/educational-attainment-age-25.csv', {'prov:label':'Education Attainment', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_education = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_education, this_script)
        doc.usage(get_education, resource_education, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
      
        education = doc.entity('dat:aqu1#education_data', {prov.model.PROV_LABEL:'Education Attainment', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(education, this_script)
        doc.wasGeneratedBy(education, get_education, endTime)
        doc.wasDerivedFrom(education, resource_education, get_education, get_education, get_education)
    
            # Income Report 
        resource_income = doc.entity('bod:34f2c48b670d4b43a617b1540f20efe3_0.csv', {'prov:label':'Low Income Population', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income, this_script)
        doc.usage(get_income, resource_income, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        income = doc.entity('dat:aqu1#income_data', {prov.model.PROV_LABEL:'Low Income Population', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(income, this_script)
        doc.wasGeneratedBy(income, get_income, endTime)
        doc.wasDerivedFrom(income, resource_income, get_income, get_income, get_income)

        # MBTA Stops Report
        resource_bus_stops = doc.entity('agh:2c00111621954fa08ff44283364bba70_0.csv?outSR=%7B%22wkid%22%3A102100%2C%22latestWkid%22%3A3857%7D', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_t_stops = doc.entity('agh:a9e4d01cbfae407fbf5afe67c5382fde_2.csv', {'prov:label':'MBTA T-Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stops, this_script)
        doc.usage(get_stops, resource_bus_stops, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_stops, resource_t_stops, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        mbta_stops = doc.entity('dat:aqu1#mbta_stops_data', {prov.model.PROV_LABEL:'MBTA Stops', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(mbta_stops, this_script)
        doc.wasGeneratedBy(mbta_stops, get_stops, endTime)
        doc.wasDerivedFrom(mbta_stops, resource_bus_stops, get_stops, get_stops, get_stops)
        doc.wasDerivedFrom(mbta_stops, resource_t_stops, get_stops, get_stops, get_stops)

        # Schools Report 
        resource_schools = doc.entity('bod:1d9509a8b2fd485d9ad471ba2fdb1f90_0.csv', {'prov:label':'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_colleges = doc.entity('bod:cbf14bb032ef4bd38e20429f71acb61a_2.csv', {'prov:label':'Boston Colleges', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, resource_schools, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_schools, resource_colleges, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        schools = doc.entity('dat:aqu1#schools_data', {prov.model.PROV_LABEL:'Boston Schools', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)
        doc.wasDerivedFrom(schools, resource_colleges, get_schools, get_schools, get_schools)
        
        repo.logout()

        return doc