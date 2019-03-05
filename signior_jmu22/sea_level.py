import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class sea_level(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = []
  writes = ['signior_jmu22.sea_level']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    url = "ftp://ftp.csiro.au/legresy/gmsl_files/CSIRO_Alt_yearly.csv"
    df = pd.read_csv(url)
    
    
    sea_level_dict = df.to_dict(orient='records')
    print(sea_level_dict)
    
    repo.dropCollection("sea_level")
    repo.createCollection("sea_level")
    repo['signior_jmu22.sea_level'].insert_many(sea_level_dict)
    repo['signior_jmu22.sea_level'].metadata({'complete': True})

    print(repo['signior_jmu22.sea_level'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()
    return {"start": startTime, "end": endTime}
    
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    return None






  
sea_level.execute()