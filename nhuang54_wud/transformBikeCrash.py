import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import radians, sqrt, sin, cos, atan2

def product(R, S):
  return [(t,u) for t in R for u in S]

def geodistance(la1, lo1, la2, lo2):
  EARTH_R = 6378.0
  la1 = radians(la1)
  lo1 = radians(lo1)
  la2 = radians(la2)
  lo2 = radians(lo2)

  long_dif = lo1 - lo2
  y = sqrt(
      (cos(la2) * sin(long_dif)) ** 2
      + (cos(la1) * sin(la2) - sin(la1) * cos(la2) * cos(long_dif)) ** 2
      )
  x = sin(la1) * sin(la2) + cos(la1) * cos(la2) * cos(long_dif)
  c = atan2(y, x)

  return EARTH_R * c



class transformBikeCrash(dml.Algorithm):
    contributor = 'nhuang54_wud'
    reads = ['nhuang54_wud.crashRecord', 'nhuang54_wud.streetlightLocation']
    writes = ['nhuang54_wud.bikeCrashStreetlight']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('nhuang54_wud', 'nhuang54_wud')

        # Get the data
        crashLocations = repo.nhuang54_wud.crashRecord.find()
        lightLocations = repo.nhuang54_wud.streetlightLocation.find()  

        # Select to only get bike crashes
        bikeCrashes = []
        for row in crashLocations:
          if row['"mode_type"'] == 'bike':
            bikeCrashes += row

        # Product between bike crashes and streetlight locations
        crashLightProduct = product()
        
        repo.dropCollection("bikeCrashStreetlight")
        repo.createCollection("bikeCrashStreetlight")
        # repo['nhuang54_wud.bikeFatality'].insert_many(json_file)
        repo['nhuang54_wud.bikeCrashStreetlight'].metadata({'complete':True})
        print(repo['nhuang54_wud.bikeCrashStreetlight'].metadata())

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
        repo.authenticate('nhuang54_wud', 'nhuang54_wud')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('vzb', 'https://data.boston.gov/dataset/')

        this_script = doc.agent('alg:nhuang54_wud#getBikeFatality', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:d326a4e3-75f2-42ac-9b32-e2920566d04c/resource/92f18923-d4ec-4c17-9405-4e0da63e1d6c', {'prov:label':'Vision Zero Fatality Records', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bikeFatality = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bikeFatality, this_script)
        doc.usage(get_bikeFatality, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':''
                  }
                  )

        bikeFatality = doc.entity('dat:nhuang54_wud#getBikeFatality', {prov.model.PROV_LABEL:'Vision Zero Fatality Records', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bikeFatality, this_script)
        doc.wasGeneratedBy(bikeFatality, get_bikeFatality, endTime)
        doc.wasDerivedFrom(bikeFatality, resource, get_bikeFatality, get_bikeFatality, get_bikeFatality)


        repo.logout()
                  
        return doc

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

if __name__ == '__main__':
    transformBikeCrash.execute()

## eof
