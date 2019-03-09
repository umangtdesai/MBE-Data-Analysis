import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import pandas as pd
from uszipcode import Zipcode
from uszipcode import SearchEngine


class transformHealth(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.health','misn15.waste']
    writes = ['misn15.waste_health']

    @staticmethod
    def execute(trial = False):
        '''Transform health and waste data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # make deepcopy of data

        waste = []
        waste = repo['misn15.waste'].find()
        waste = copy.deepcopy(waste)

        health = []
        health = repo['misn15.health'].find()
        health = copy.deepcopy(health)

        health_pd = pd.DataFrame(health)
        health_pd = health_pd[pd.isnull(health_pd['tractfips']) == False]
        health_pd = health_pd[pd.isnull(health_pd['data_value']) == False]

        #filter health
        health_subset = health_pd[["category", "measure", "data_value_unit", "data_value", "categoryid", "geolocation", "measureid", "short_question_text", "tractfips"]]

        # convert to a list
        health_list = []
        for x in range(len(health_subset)):
            health_list += [list(health_subset.iloc[x, :])]

        # add zipcodes
        search = SearchEngine(simple_zipcode=True)
        for x in health_list:
            long = round(x[5]["coordinates"][0], 2)
            lat = round(x[5]["coordinates"][1], 2)
            result = search.by_coordinates(lat, long, returns = 1)
            result = result[0]
            x += [result.zipcode]                                         

        # get the product of waste and health data sets and project to         
        product = []
        for row in waste:
            for i in health_list:
                product += [[row, i]]
        
        projection = [(x[0]['Name'], x[0]['Address'], x[1][0], x[1][3], x[1][6], x[1][-1]) for x in product if '0'+str(x[0]['ZIP Code']) == x[1][-1]]

        # filter out prevention; we only want actual illness
        no_prev = [x for x in projection if x[2] != "Prevention"]

        #get all the different types of illnesses
        keys = []
        for x in no_prev:
            keys += [x[4]]
        keys = set(keys)

        # append a dictionary of all illnesses and prevalence rates for every waste site

        agg_health = []
        prev = ''
        dict_disease = {}
        dict_disease[no_prev[0][4]] = [no_prev[0][3]]
        for x in no_prev:
            if x[0] == prev and x[4] not in dict_disease.keys():
                dict_disease[x[4]] = [x[3]]
                prev = x[0]
            elif x[0] == prev and x[4] in dict_disease.keys():
                dict_disease[x[4]] += [x[3]]
                prev = x[0]
            else:
                #print(dict_disease)
                agg_health += [[x[0], x[1], x[5], dict_disease]]
                #agg_health += [dict_disease]
                dict_disease = {}
                dict_disease[x[4]] = [x[3]]
                prev = x[0]

        
        repo.dropCollection("misn15.waste_health")
        repo.createCollection("misn15.waste_health")

        for x in agg_health:
            entry = {'Name': x[0], "Address": x[1], "Zip code": x[2], "Disease": x[3]}
            repo['misn15.waste_health'].insert_one(entry)        
            
        repo['misn15.waste_health'].metadata({'complete':True})
        print(repo['misn15.waste_health'].metadata())

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

        this_script = doc.agent('alg:misn15#transformHealth', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        health = doc.entity('dat:misn15.health', {'prov:label':'Health', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        waste = doc.entity('dat:misn15.waste', {'prov:label': 'Health', prov.model.PROV_TYPE: 'ont: DataResource', 'ont:Extension':'json'})                                              
        health_waste = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(health_waste, this_script)
        doc.usage(health_waste, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   }
                  )

        getHealth = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getHealth, this_script)
        doc.usage(getHealth, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                   }
                  )



      
        crime_data = doc.entity('dat:misn15#RetrieveCrime', {prov.model.PROV_LABEL:'Boston Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_data, this_script)
        doc.wasGeneratedBy(crime_data, get_crime, endTime)
        doc.wasDerivedFrom(crime_data, resource, get_crime, get_crime, get_crime)
                  
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
