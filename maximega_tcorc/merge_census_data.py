import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class merge_census_data(dml.Algorithm):
    contributor = 'maximega_tcorc'
    reads = ['maximega_tcorc.census_tracts', 'maximega_tcorc.census_income']
    writes = ['maximega_tcorc.income_with_tracts']


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        repo_name = merge_census_data.writes[0]
        # ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        # ----------------- Retrieve data from Mongodb -----------------
        incomes = repo.maximega_tcorc.census_income
        tracts = repo.maximega_tcorc.census_tracts

        repo.dropCollection('income_with_tracts')
        repo.createCollection('income_with_tracts')

        # ----------------- Remove entries from incomes DB that are not from the year 2016 -----------------
        tract_with_income = {}
        for inc in incomes.find():
            if inc['year'] != 2016:
                incomes.remove(inc)

        # ----------------- Merge Census Tract info with AVG income per tract -----------------
        for tract in tracts.find():
            for income in incomes.find():
                tract_num_income = income['geo']
                tract_num_income = tract_num_income[10:]
                # ----------------- Normalizing naming conventions for census tracts according to link below (bullets 4c & 4d in document) -----------------
                # ----------------- http://www.geo.hunter.cuny.edu/~amyjeu/gtech201/spring10/lab8_census.pdf -----------------
                if (tract_num_income[0:2] == '85'): tract_num_income = '5' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '81'): tract_num_income = '4' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '47'): tract_num_income = '3' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '05'): tract_num_income = '2' + tract_num_income[2:]
                elif (tract_num_income[0:2] == '61'): tract_num_income = '1' + tract_num_income[2:]
                tract_num_tract = tract['BoroCT2010']
                # ----------------- If census tracts are equal, create new data object to load into DB -----------------
                if int(tract_num_income) == tract_num_tract:
                    tract_with_income[tract_num_income] = {'income': income['income'], 'nta': tract['NTACode'], 'nta_name': tract['NTAName'], 'multi_polygon': tract['the_geom']}

        # ----------------- Reformat data for mongodb insertion -----------------
        insert_many_arr = []
        for key in tract_with_income.keys():
            insert_many_arr.append(tract_with_income[key])
            
        # ----------------- Data insertion into Mongodb ------------------
        repo.dropCollection('income_with_tracts')
        repo.createCollection('income_with_tracts')
        repo[repo_name].insert_many(insert_many_arr)
        repo[repo_name].metadata({'complete':True})
        print(repo[repo_name].metadata())
        
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
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        #resources:
        doc.add_namespace('dng', 'https://data.ny.gov/resource/hvwh-qtfg.json')
        doc.add_namespace('dcu', 'https://data.cityofnewyork.us/resource/q2z5-ai38.json')
        #agent
        this_script = doc.agent('alg:maximega_tcorc#stations_with_neighborhoods', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        #defining neighborhoods entity
        mt_neighborhoods = doc.entity('dat:maximega_tcorc#neighborhoods', {'prov:label':'NYC Neighborhoods', prov.model.PROV_TYPE:'ont:DataResource'})
        #defining stations entity
        mt_stations = doc.entity('dat:maximega_tcorc#stations', {'prov:label':'NYC Subway Stations', prov.model.PROV_TYPE:'ont:DataResource'})
        
        get_neighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_station = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood, this_script)
        doc.wasAssociatedWith(get_station, this_script)
        doc.usage(get_neighborhood, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_station, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        neighborhoods = doc.entity('dat:maximega_tcorc#neighborhoods', {prov.model.PROV_LABEL:'NYC Neighborhood Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, get_neighborhoods, endTime)
        doc.wasDerivedFrom(neighborhoods, resource, get_neighborhoods, get_neighborhoods, get_neighborhoods)

        stations = doc.entity('dat:maximega_tcorc#stations', {prov.model.PROV_LABEL:'NYC Subway Station Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, resource, get_stations, get_stations, get_stations)

        repo.logout()
                  
        return doc

merge_census_data.execute()