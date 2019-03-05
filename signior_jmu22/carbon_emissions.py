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

    url = 'http://datamechanics.io/data/signior_jmu22/co2ppm.json'
    response = urllib.request.urlopen(url).read().decode("utf-8")
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
    return None






  