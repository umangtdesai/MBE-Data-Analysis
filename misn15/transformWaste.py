import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import pandas as pd
from pyproj import Proj, transform
from uszipcode import Zipcode
from uszipcode import SearchEngine
import requests
import zipcodes

class transformWaste(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.waste', 'misn15.oil']
    writes = ['misn15.waste_merged']

    @staticmethod
    def execute(trial = False):
        '''Transform waste data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        oil = []
        oil = repo['misn15.oil'].find()
        oil = copy.deepcopy(oil)

        #filter oil dataset
        oil_subset = []
        for x in oil:
            if x['properties']['TOWN'] == 'BOSTON':
                oil_subset += [[x['properties']['NAME'], x['properties']['ADDRESS'], x['geometry']['coordinates']]]

        # get csr for coordinate search
        inProj = Proj(init='epsg:26986')
        outProj = Proj(init='epsg:4326')
        

        # project coordinates as US census tract number and zipcode
        search = SearchEngine(simple_zipcode=True)
        for x in oil_subset:
            long, lat = transform(inProj, outProj, x[2][0], x[2][1])
            x[2][0] = round(long, 2)
            x[2][1] = round(lat, 2)
            result = search.by_coordinates(lat, long, returns = 1)
            result = result[0]
            x += [result.zipcode]

        # get FIPS census tract number using coordinates
        for x in oil_subset:
            lat = x[2][1]
            lon = x[2][0]
            params = urllib.parse.urlencode({'latitude': lat, 'longitude':lon, 'format':'json'})
            url = 'https://geo.fcc.gov/api/census/block/find?' + params
            response = requests.get(url)
            data = response.json()
            geoid = data['Block']['FIPS'][0:11]
            x += [geoid]
            
        waste = []
        waste = repo['misn15.waste'].find()
        waste = copy.deepcopy(waste)

        waste_list = []
        for x in waste:
            waste_list += [[x['Name'], x['Address'], [0, 0], x['ZIP Code']]]

        # get coordinates and fips for waste data
        for x in waste_list:
            if x[3][0] != '0':
                zipcode_num = '0' + x[3]
            elif len(x[3]) != 5:
                zipcode_num = x[3][0:5]
            if x[3][0] == 0 and x[3][1] == 0:
                zipcode_num = x[3][1:6]
            zipcode = zipcodes.matching(zipcode_num)
            if len(zipcode) != 0:
                x[2][0] = zipcode[0]['long']
                x[2][1] = zipcode[0]['lat']           

        # get FIPS census tract number for waste data
        for x in waste_list:
            lat = x[2][1]
            lon = x[2][0]
            if lat != 0:
                params = urllib.parse.urlencode({'latitude': lat, 'longitude':lon, 'format':'json'})
                url = 'https://geo.fcc.gov/api/census/block/find?' + params
                response = requests.get(url)
                data = response.json()
                geoid2 = data['Block']['FIPS'][0:11]
                x += [geoid2]
            else:
                x += ['0']             

        # merge oil sites with hazardous waste sites
            
        waste_merged = oil_subset + waste_list

        repo.dropCollection("misn15.waste_merged")
        repo.createCollection("misn15.waste_merged")

        for x in waste_merged:
            entry = {'Name': x[0], 'Address': x[1], 'Coordinates': x[2], 'Zip Code': x[3], 'FIPS': x[4]}
            repo['misn15.waste_merged'].insert_one(entry)        

        repo['misn15.waste_merged'].metadata({'complete':True})
        print(repo['misn15.waste_merged'].metadata())

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
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://docs.digital.mass.gov/dataset/massgis-data-massdep-tier-classified-oil-andor-hazardous-material-sites-mgl-c-21e')

        this_script = doc.agent('alg:misn15#waste_merged', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:Boston_waste_merged', {'prov:label':'Boston_waste_merged', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_merged = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_merged, this_script)
        doc.usage(get_merged, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                        }
                  )
        merged_data = doc.entity('dat:misn15#MergedWaste', {prov.model.PROV_LABEL:'Waste Sites', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(merged_data, this_script)
        doc.wasGeneratedBy(merged_data, get_merged, endTime)
        doc.wasDerivedFrom(merged_data, resource, get_merged, get_merged, get_merged)
                  
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
