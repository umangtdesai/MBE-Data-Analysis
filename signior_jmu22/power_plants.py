import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd






class power_plants(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = []
  writes = ['signior_jmu22.power_plants']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    url = 'http://datamechanics.io/data/signior_jmu22/global_power_plant_database.csv'
    df = pd.read_csv(url)
    new_df = df.filter(['country_long', 'name', 'fuel1', 'fuel2', 'fuel3', 'fuel4', 'comissioning_year'], axis=1)
    # print(new_df.head(5))
    power_plants_dict = new_df.to_dict(orient='records')
    
    repo.dropCollection("power_plants")
    repo.createCollection("power_plants")
    repo['signior_jmu22.power_plants'].insert_many(power_plants_dict)
    repo['signior_jmu22.power_plants'].metadata({'complete': True})

    print(repo['signior_jmu22.power_plants'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()
    return {"start": startTime, "end": endTime}
    
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    return None






  
power_plants.execute()