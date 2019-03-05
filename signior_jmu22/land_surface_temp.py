

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from datapackage import Package, Resource

class land_surface_temp(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = []
  writes = ['signior_jmu22.land_surface_temp']

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    # Set up database connection
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('signior_jmu22', 'signior_jmu22')

    package = Package('https://datahub.io/core/global-temp-anomalies/datapackage.json')

    raw_data = package.get_resource('global-temp-annual_csv').read(keyed=True, cast=False)

    df = pd.DataFrame(raw_data)
    new_df = df.filter(['Year', 'Land'], axis=1)
    
    land_temp_dict = new_df.to_dict(orient='records')
    print(land_temp_dict)
    
    repo.dropCollection("land_surface_temp")
    repo.createCollection("land_surface_temp")
    repo['signior_jmu22.land_surface_temp'].insert_many(land_temp_dict)
    repo['signior_jmu22.land_surface_temp'].metadata({'complete': True})

    print(repo['signior_jmu22.land_surface_temp'].metadata())

    repo.logout()

    endTime = datetime.datetime.now()
    return {"start": startTime, "end": endTime}
    
    
  
  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    return None






  
land_surface_temp.execute()