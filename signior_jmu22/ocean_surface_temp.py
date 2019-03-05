import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

class ocean_surface_temp(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = []
  writes = ['signior_jmu22.ocean_surface_temp']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    url = 'http://datamechanics.io/data/signior_jmu22/sea_surface_temp_anomaly.csv'
    df = pd.read_csv(url)
    new_df = df.filter(['Year', 'Annual anomaly'], axis=1)
    # print(new_df.head(5))
    ocean_temp_dict = new_df.to_dict(orient='records')
    
    repo.dropCollection("ocean_surface_temp")
    repo.createCollection("ocean_surface_temp")
    repo['signior_jmu22.ocean_surface_temp'].insert_many(ocean_temp_dict)
    repo['signior_jmu22.ocean_surface_temp'].metadata({'complete': True})

    print(repo['signior_jmu22.power_plants'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()
    return {"start": startTime, "end": endTime}
    
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    return None






  
ocean_surface_temp.execute()