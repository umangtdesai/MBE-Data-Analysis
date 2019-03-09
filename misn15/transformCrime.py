import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import copy
import pandas as pd
from census import Census
from us import states

class transformDem(dml.Algorithm):
    contributor = 'misn15'
    reads = ['misn15.health','misn15.waste', 'misn15.zipcode']
    writes = ['misn15.waste_health']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        c = Census("d07b9c8b06ff352d4f8217ddc4f15737fc6c867c", year=2018)
        data = c.acs5.state_county_tract('B06011_001E', states.MA.fips, '025', Census.ALL)
        
        zipcodes = []
        zipcodes = repo['misn15.zipcodes'].find()
        zipcodes = [x for x in zipcodes]

        waste = []
        waste = repo['misn15.waste'].find()
        waste_1 = copy.deepcopy(waste)

        crime = []
        crime = repo['misn15.crime'].find()

        data_pd =  pd.DataFrame(data)

        # add columns together
        data_pd['full_tract'] = data_pd[['state', 'county', 'tract']].apply(lambda x: ''.join(x), axis=1)
        data_pd['zipcode'] = -1
        
        # match median incomes to zipcodes; some zipcodes will have multiple median incomes
        zips = {}
        for i in range(len(data_pd)):
            for j in range(len(zipcodes)):
                if int(data_pd.iloc[i]['full_tract']) == zipcodes[j]['tract']:
                    if zipcodes[j]['zip'] not in zips and data_pd.iloc[i]['B06011_001E'] != -666666666.0:
                    #data_pd.loc[i, 'zipcode'] += [[zipcodes[j]['zip']]]
                    #zips += [data_pd.iloc[i]['B06011_001E'], zipcodes[j]['zip']]
                        zips[zipcodes[j]['zip']] = [str(data_pd.iloc[i]['B06011_001E'])]
                    elif data_pd.iloc[i]['B06011_001E'] != -666666666.0:
                        zips[zipcodes[j]['zip']].append(str(data_pd.iloc[i]['B06011_001E']))

        # for each zipcode get number of average median incomes and their sum to find the mean; projection and aggregation    
        incomes_list = []
        sum_incomes = 0       
        for key, value in zips.items():
            sum_incomes = 0
            for y in value:
                sum_incomes += float(y)
            incomes_list += [[key, len(value), sum_incomes]]

        # get mean income for each zipcode
        
        mean_incomes = [[x[0], x[2]/x[1]] for x in incomes_list]
                            
        # product waste and median income
        product = []
        for row in waste_1:
            for i in range(len(mean_incomes)):
                product += [[row, mean_incomes[i]]]

        # selection and projection
        projection = [(x[0]['Address'], x[0]['ZIP Code'], x[1][1]) for x in product if str(x[1][0]) == x[0]['ZIP Code']]


        # aggregate number of waste sites for each zip code
        keys = []
        for x in projection:
            keys += [x[1]]
        keys = set(keys)

        agg_waste = [(key, sum([1 for v in projection if v[1] == key]), v[2]) for key in keys]


        # project waste sites and crime
        
        return [(key, f([1 for v in projection if v[1] == key])) for key in keys]
        

        # make deepcopy of the data
        waste_1 = copy.deepcopy(waste)
        health_1 = copy.deepcopy(health)
        zipcodes_1 = copy.deepcopy(zipcodes)

        health_pd = pd.DataFrame(health)
        health_pd = health_pd[pd.isnull(health_pd['tractfips']) == False]
        health_pd['zipcode'] = 0
        zipcodes = [x for x in zipcodes]

##        health_zips = []
##        for i in health_1:
##            for j in zipcodes_1:
##		if int(i['tractfips']) == j['tract']:
##			add = (i['category'], j['zip'])
##			health_zips.append(add)
                    

        
        # add zipcodes to health data set
        for i in range(len(health_pd)):
            for j in range(len(zipcodes)):
                if int(health_pd.iloc[i]['tractfips']) == zipcodes[j]['tract']:
                    health_pd.iloc[i]['zipcode'] = int(zipcodes[j]['zip'])
                    break
                
        # filter health dataset
        #health_subset = health_pd.loc[:, ["category", "measure", "data_value_unit", "data_value", "categoryid", "measureid", "short_question_text", "tractfips", "zipcode"]]


        # get the product of waste and health data sets
        
        product = []
        for row in waste_1:
            for i in range(len(health_pd)):
                product += [[row, health_pd.iloc[i]]]
        
        waste_health = product(health_subset, waste)
        
        fields = (x[0]['Name'], x[1]['categoryid'])
        projection = [(x[0]['Name']) for x in product if str(x[1]['zipcode']) in x[0]['ZIP Code'])]

        repo.dropCollection("misn15.waste_health")
        repo.createCollection("misn15.waste_health")

        for x in projection:
            entry = {'Name': x}
            repo['misn15.waste_health'].insert_one(entry)
         
           # add = (row['Name'], row['Address'])
           # waste_list.append(add)
            
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
        doc.add_namespace('bdp', 'https://data.boston.gov/api/3/action/datastore_search_sql?sql=SELECT * from "12cb3883-56f5-47de-afa5-3b1cf61b257b" WHERE CAST(year AS Integer) > 2016')

        this_script = doc.agent('alg:misn15#crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:Boston_crime', {'prov:label':'Boston_crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query': '?sql=SELECT * from "12cb3883-56f5-47de-afa5-3b1cf61b257b" WHERE CAST(year AS Integer) > 2016'
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
