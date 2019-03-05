import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import urllib.request

class carbon_emissions(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = []
  writes = ['signior_jmu22.carbon_emissions']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    url = 'http://datamechanics.io/data/signior_jmu22/co2ppm.json' # grab dataset from datamechanics.io
    response = urllib.request.urlopen(url).read().decode("utf-8") # open with python request library
    r = json.loads(response) 
    s = json.dumps(r, sort_keys=True, indent=2)
    repo.dropCollection("carbon_emissions")
    repo.createCollection("carbon_emissions")
    repo['signior_jmu22.carbon_emissions'].insert_many(r)
    repo['signior_jmu22.carbon_emissions'].metadata({'complete': True})

    print(repo['signior_jmu22.carbon_emissions'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()

    return {"start": startTime, "end": endTime}
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')
    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/signior_jmu22') # The scripts are in <folder>#<filename> format
    doc.add_namespace('dat', 'http://datamechanics.io/data/signior_jmu22' ) # The datasets are in <user>#<collection> format
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retreival', 'Query', or 'Computation'
    doc.add_namespace('log', 'http://datamechanics.io/log/')

    this_script = doc.agent('alg:signior_jmu22#carbon_emissions', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    resource = doc.entity('dat:carbon_emissions', {'prov:label': 'Global CO2 emissions data 1980 - 2017', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'csv'})
    get_carbon_emissions = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(get_carbon_emissions, this_script)
    doc.usage(get_carbon_emissions, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retreival'})
    
    carbon_emissions = doc.entity('dat:signior_jmu22#carbon_emissions', {prov.model.PROV_LABEL: 'Global Carbon Emissions', prov.model.PROV_TYPE: 'ont:DataSet'})
    doc.wasAttributedTo(carbon_emissions, this_script)
    doc.wasGeneratedBy(carbon_emissions, get_carbon_emissions, endTime)
    doc.wasDerivedFrom(carbon_emissions, resource, get_carbon_emissions, get_carbon_emissions, get_carbon_emissions)

    repo.logout()


    return doc



# comment this when submitting, just for testing purposes
# carbon_emissions.execute()
# doc = carbon_emissions.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
  