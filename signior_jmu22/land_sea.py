#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 18:17:14 2019

@author: jeffreymu
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd


class land_sea(dml.Algorithm):
  contributor = 'signior_jmu22'
  reads = ['signior_jmu22.land_surface_temp', 'signior_jmu22.ocean_surface_temp']
  writes = ['signior_jmu22.land_sea']
  
  @staticmethod
  def execute(trial = False):
      startTime = datetime.datetime.now()
      
      client = dml.pymongo.MongoClient()
      repo = client.repo
      repo.authenticate('signior_jmu22', 'signior_jmu22')
      
      
      #creates list of dictionary object
      land_surface_temp = list(repo.signior_jmu22.land_surface_temp.find())

    
      #print(land_surface_temp)
      ocean_surface_temp = list(repo.signior_jmu22.ocean_surface_temp.find())
      
      #print(land_surface_temp[0])
      #print(ocean_surface_temp[0])
      
      #formats the year in land_surface_Temp
      for i in land_surface_temp:
          i["Year"] = i["Year"][0:4]
        
          #print(type(i))
      #formats the year in ocean_surface_Temp
      for i in ocean_surface_temp:
          i["Year"] = str(i["Year"])[0:4]
          i["Annual anomaly"] = "{0:.2f}".format(i["Annual anomaly"])
          i["Ocean"] = i.pop("Annual anomaly")
          #print(i)
    
    
      land_ocean_product = land_sea.product(land_surface_temp, ocean_surface_temp)      
      #print(land_ocean_product[0])
     
      land_ocean_select = land_sea.select(land_ocean_product, lambda t: t[0].get("Year") == t[1].get("Year"))
      #print(land_ocean_select)
      
      
      
      #z = {**land_ocean_select[0][0],**land_ocean_select[0][1]}
      #print(z)
      templist = []
      count = 0
      index = 0 
      for i in land_ocean_select:
          
          z= {**land_ocean_select[count][index],**land_ocean_select[count][index+1]}
          index= 0
          count+=1
          templist.append(z)
      
        
        
      #fx = land_sea.project(templist, lambda t: (t[0],t[1]))
      #form of projection
      finalList= []
      for i in templist:
          keys = ["Year", "Ocean", "Land"]
          filtered = dict(zip(keys, [i[k] for k in keys]))
          finalList.append(filtered)
      #print(finalList)
      
       # below block adds the dataset to the repo collection
      repo.dropCollection("land_sea")
      repo.createCollection("land_sea")
      repo['signior_jmu22.land_sea'].insert_many(finalList)
      repo['signior_jmu22.land_sea'].metadata({'complete': True})

      print(repo['signior_jmu22.land_sea'].metadata())

      repo.logout()

      endTime = datetime.datetime.now()
      return {"start": startTime, "end": endTime}
          
          
      
   
      
    
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
      return None

  def union(R, S):
      return R + S

  def difference(R, S):
      return [t for t in R if t not in S]

  def intersect(R, S):
      return [t for t in R if t in S]

  def project(R, p):
      return [p(t) for t in R]

  def select(R, s):
      return [t for t in R if s(t)]
     
  def product(R, S):
      return [(t,u) for t in R for u in S]
 
    
  def aggregate(R, f):
      keys = {r[0] for r in R}
      return [(key, f([v for (k,v) in R if k == key])) for key in keys]

  

  
    
land_sea.execute()