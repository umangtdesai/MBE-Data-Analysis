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
    reads = ['misn15.waste', 'misn15.zipcode', 'misn15.income']
    writes = ['misn15.waste_income']

    @staticmethod
    def execute(trial = False):
        '''Retrieve crime data for city of Boston'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('misn15', 'misn15')

        # census returns median income by census tract number
        c = Census("a839d0b0a206355591f27266b5205596d1bae45c", year=2017)
        data = c.acs5.state_county_tract('B06011_001E', states.MA.fips, '025', Census.ALL)
        
        zipcodes = []
        zipcodes = repo['misn15.zipcodes'].find()
        zipcodes = [x for x in zipcodes]

        waste = []
        waste = repo['misn15.waste'].find()
        waste = copy.deepcopy(waste)

        income = []
        income = repo['misn15.income'].find()
        data_pd =  pd.DataFrame(income)

        # add columns together
        data_pd['full_tract'] = data_pd[['state', 'county', 'tract']].apply(lambda x: ''.join(x), axis=1)
        data_pd['zipcode'] = -1
        
        # match median incomes to zipcodes; some zipcodes will have multiple median incomes
        zips = {}
        for i in range(len(data_pd)):
            for j in range(len(zipcodes)):
                if int(data_pd.iloc[i]['full_tract']) == zipcodes[j]['tract']:
                    if zipcodes[j]['zip'] not in zips and data_pd.iloc[i]['B06011_001E'] != -666666666.0:
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
                            
        # product between waste and median income
        product = []
        for row in waste:
            for i in range(len(mean_incomes)):
                product += [[row, mean_incomes[i]]]

        # selection and projection
        projection = [(x[0]['Address'], x[0]['ZIP Code'], x[1][1]) for x in product if str(x[1][0]) == x[0]['ZIP Code']]

        # aggregate number of waste sites for each zip code
        keys = []
        for x in projection:
            keys += [x[1]]
        keys = set(keys)

        agg_waste = [[key, sum([1 for x in projection if x[1] == key])] for key in keys]

        # add mean incomes to zipcodes
        for x in agg_waste:
            for y in mean_incomes:
                if x[0] == str(y[0]):
                    x += [round(y[1], 2)]            

                    
        repo.dropCollection("misn15.waste_income")
        repo.createCollection("misn15.waste_income")

        for x in agg_waste:
            entry = {'Zip code': x[0], '# of Waste Sites': x[1], 'Median Income': x[2]}
            repo['misn15.waste_income'].insert_one(entry)
         

        repo['misn15.waste_income'].metadata({'complete':True})
        print(repo['misn15.waste_income'].metadata())

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
