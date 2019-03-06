import requests
import json
import dml
import prov.model
import datetime
import uuid
import csv

class CDAge(dml.Algorithm):
    contributor = 'gengtaox_gengxc_jycai_ruoshi'
    reads = []
    writes = ['gengtaox_gengxc_jycai_ruoshi.CDAge',]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('gengtaox_gengxc_jycai_ruoshi', 'gengtaox_gengxc_jycai_ruoshi')
        
        url = "https://www.census.gov/mycd/application/bin/functs_easystats.php"
        
        # Init the dict
        CD_dict = []
        for i in range(10):
            CD_dict.append({
                "District": i,
                "Age": {}
            })

        for i in range(1, 10):
            params={
                    "call": "get_values",
                    "geo_type": "CONGRESSIONAL_DISTRICT",
                    "geo_level_1": "25",
                    "geo_level_2": str(i).zfill(2),
                    "url": "https://api.census.gov/data/2017/acs/acs1/profile",
                    "tableid": "99_mcd_people",
            }
            response = requests.get(url, params=params).json()
            CD_dict[i]['Age']["16-19"] = {
                "U": int(response['DP05_0008E']),
            }

        columns = ["18-24", "25-34", "35-49", "50-64", "65+", "unknown"]

        nonRegData = []
        with open('gengtaox_gengxc_jycai_ruoshi/non_registered_CD.csv') as csvfile:
            reader = csv.reader(csvfile)
            nonRegData = list(reader)
        nonRegData = nonRegData[1:]

        regData = []
        with open('gengtaox_gengxc_jycai_ruoshi/registered_voters_CD.csv') as csvfile:
            reader = csv.reader(csvfile)
            regData = list(reader)
        regData = regData[1:]

        for i in range(1, 10):
            for col in range(1, 7):
                CD_dict[i]['Age'][columns[col-1]] = {
                    "M": int(nonRegData[i][col].replace(',','')) + int(regData[i][col].replace(',','')),
                    "F": int(nonRegData[i][col+6].replace(',','')) + int(regData[i][col+6].replace(',','')),
                    "U": int(nonRegData[i][col+12].replace(',','')) + int(regData[i][col+12].replace(',',''))
                }

        CD_dict = CD_dict[1:]

        repo.dropCollection("CDAge")
        repo.createCollection("CDAge")
        
        repo['gengtaox_gengxc_jycai_ruoshi.CDAge'].insert_many(CD_dict)
        repo['gengtaox_gengxc_jycai_ruoshi.CDAge'].metadata({'complete':True})
        print(repo['gengtaox_gengxc_jycai_ruoshi.CDAge'].metadata())

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

        doc.add_namespace('ont', 'https://www.census.gov/mycd/') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('ucb', 'https://www.census.gov/')
        
        doc.add_namespace('alg', 'https://www.census.gov/mycd/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'https://www.census.gov/mycd/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('log', 'https://www.census.gov/mycd/') # The event log.

        this_script = doc.agent('alg:gengtaox_gengxc_jycai_ruoshi#CDAge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('ucb:wc8w-nujj', {'prov:label':'My Congressional District', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_CDAge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_CDAge, this_script)
        doc.usage(get_CDAge, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?st=25&cd=01'
                  }
                  )

        CDAge = doc.entity('dat:gengtaox_gengxc_jycai_ruoshi#CDAge', {prov.model.PROV_LABEL:'Age By Congress District', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CDAge, this_script)
        doc.wasGeneratedBy(CDAge, get_CDAge, endTime)
        doc.wasDerivedFrom(CDAge, resource, get_CDAge, get_CDAge, get_CDAge)
                  
        return doc


## eof