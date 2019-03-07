# flood.py
# created on Feb 27, 2019

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
import sys
import traceback
import csv
import xmltodict

class flood(dml.Algorithm):
	contributor = "dezhouw_ghonigsb"
	reads       = []
	writes      = ["nine_inch_sea_level_rise_1pct_annual_flood",
				   "nine_inch_sea_level_rise_high_tide",
				   "zoning_subdistricts"]

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("dezhouw_ghonigsb", "dezhouw_ghonigsb")

		# Drop all collections
		collection_names = repo.list_collection_names()
		for collection_name in collection_names:
			repo.dropCollection(collection_name)

		# nine_inch_sea_level_rise_1pct_annual_flood
		collection_name = "nine_inch_sea_level_rise_1pct_annual_flood"
		url = "https://opendata.arcgis.com/datasets/74692fe1b9b24f3c9419cd61b87e4e3b_7.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		repo["dezhouw_ghonigsb."+collection_name].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb."+collection_name].metadata())

		# nine_inch_sea_level_rise_high_tide
		collection_name = "nine_inch_sea_level_rise_high_tide"
		url = "https://opendata.arcgis.com/datasets/74692fe1b9b24f3c9419cd61b87e4e3b_8.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		repo["dezhouw_ghonigsb."+collection_name].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb."+collection_name].metadata())

		# zoning_subdistricts
		collection_name = "zoning_subdistricts"
		url = "https://opendata.arcgis.com/datasets/b601516d0af44d1c9c7695571a7dca80_0.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		repo["dezhouw_ghonigsb."+collection_name].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb."+collection_name].metadata())

		# census
		collection_name = "zillow_boston_neighborhood"
		params = {
			'zws-id'   : dml.auth["census"]["Zillow API"]["zws-id"],
			'state'    : 'ma',
			'city'     : 'boston',
			'childtype': 'neighborhood'
		}
		args = urllib.parse.urlencode(params).encode('UTF-8')
		url = "https://www.zillow.com/webservice/GetRegionChildren.htm?"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, args, context=gcontext).read().decode("utf-8")
		r = xmltodict.parse(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		repo["dezhouw_ghonigsb."+collection_name].metadata({'complete':True})
		print(repo["dezhouw_ghonigsb."+collection_name].metadata())
		

		# uber info
		# collection_name = "uber_info"
		# url = "https://movement.uber.com/travel-times/6_taz"
		# gcontext = ssl.SSLContext()
		# response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		# r = json.loads(response)
		# repo.createCollection(collection_name)
		# repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		# repo["dezhouw_ghonigsb."+collection_name].metadata({'complete':True})
		# print(repo["dezhouw_ghonigsb."+collection_name].metadata())

		# uber transportation
		# collection_name = "uber_travel_times_by_month"
		# url = "https://d3sc9lyn9f6mdg.cloudfront.net/6/taz/2018/4/boston-taz-2018-4-All-MonthlyAggregate.csv?Expires=1551910879&Signature=K4Is-Hu1eJqbcxFgqMjxQ9ERdzewRSv8X~EY8mZrSOygqbWbP~3HCdb3hgQ1zmoBl3WuveK7dEoayci2f7iuz2cHu3AJMvMoxW0RWuW7G39s1QhL5CAVY0G1N9zOAPXlxNz9QWuBsRnWv~vNiSAoC-e4lpASrBtihtcsXXKOvOdk~x64Umr-DOB0IzOuyMyzwN9PPVFmdmnzAdZAHTwDw0QVxXpTyCou8HchT7-ERmFDrVwsA34YFsaXvprFVKBJbgQgHPYup4~sRT4enMw8d1hsXbYjwsr~VzyM55DMR06rQ4r41NHyNV8od7CLZhfAtQDwkSy7Jzyu1XxsCHd0mQ__&Key-Pair-Id=APKAJW2WGL2ZAXG7WKYQ"
		# gcontext = ssl.SSLContext()
		# response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		# for row in response:
		# 	print(row)
		# 	break




        # Disconnect database for data safety
		repo.logout()

		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

	@staticmethod
	def convertCSVtoJSON():
		csvfile = open("travel_times.csv", 'r')
		jsonfile = open("travel_times.json", 'w')

		fieldnames = ['sourceid', 'dstid', 'month', 'mean_travel_time', 'standard_deviation_travel_time', 'geometric_mean_travel_time', 'geometric_standard_deviation_travel_time']
		reader = csv.DictReader(csvfile, fieldnames)
		next(reader, None) # Skip header

		for row in reader:
			json.dump(row, jsonfile)
			jsonfile.write('\n')

		csvfile.close()
		jsonfile.close()


try:
	print(flood.execute())
	# flood.convertCSVtoJSON()
except Exception as e:
	traceback.print_exc(file = sys.stdout)
finally:
	print("Safely close")
