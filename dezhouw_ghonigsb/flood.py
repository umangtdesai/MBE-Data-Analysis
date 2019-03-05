# flood.py
# created on Feb 27, 2019

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl

class flood(dml.Algorithm):
	contributor = "dezhouw_ghonigsb"
	reads       = []
	writes      = []

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("dezhouw_ghonigsb", "dezhouw_ghonigsb")

		# 9inch_sea_level_rise_1pct_annual_flood
		url = "https://opendata.arcgis.com/datasets/74692fe1b9b24f3c9419cd61b87e4e3b_7.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.dropCollection("9inch_sea_level_rise_1pct_annual_flood")
		repo.createCollection("9inch_sea_level_rise_1pct_annual_flood")
		repo["dezhouw_ghonigsb.9inch_sea_level_rise_1pct_annual_flood"].insert_many(r)
		repo["dezhouw_ghonigsb.9inch_sea_level_rise_1pct_annual_flood"].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb.9inch_sea_level_rise_1pct_annual_flood"].metadata())

		# 9inch_sea_level_rise_high_tide
		url = "https://opendata.arcgis.com/datasets/74692fe1b9b24f3c9419cd61b87e4e3b_8.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.dropCollection("9inch_sea_level_rise_high_tide")
		repo.createCollection("9inch_sea_level_rise_high_tide")
		repo["dezhouw_ghonigsb.9inch_sea_level_rise_high_tide"].insert_many(r)
		repo["dezhouw_ghonigsb.9inch_sea_level_rise_high_tide"].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb.9inch_sea_level_rise_high_tide"].metadata())

		# zoning_subdistricts
		url = "https://opendata.arcgis.com/datasets/b601516d0af44d1c9c7695571a7dca80_0.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.dropCollection("zoning_subdistricts")
		repo.createCollection("zoning_subdistricts")
		repo["dezhouw_ghonigsb.zoning_subdistricts"].insert_many(r)
		repo["dezhouw_ghonigsb.zoning_subdistricts"].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb.zoning_subdistricts"].metadata())

		# census
		

		# uber transportation


        # Disconnect database for data safety
		repo.logout()

		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass


# flood.execute()
