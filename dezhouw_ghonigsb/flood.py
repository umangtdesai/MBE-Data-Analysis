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
import io

class flood(dml.Algorithm):
	contributor = "dezhouw_ghonigsb"
	reads       = []
	writes      = ["nine_inch_sea_level_rise_1pct_annual_flood",
				   "nine_inch_sea_level_rise_high_tide",
				   "zoning_subdistricts",
				   "zillow_boston_neighborhood"]

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
		print("Success: [{}]".format(collection_name))

		# nine_inch_sea_level_rise_high_tide
		collection_name = "nine_inch_sea_level_rise_high_tide"
		url = "https://opendata.arcgis.com/datasets/74692fe1b9b24f3c9419cd61b87e4e3b_8.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		print("Success: [{}]".format(collection_name))

		# zoning_subdistricts
		collection_name = "zoning_subdistricts"
		url = "https://opendata.arcgis.com/datasets/b601516d0af44d1c9c7695571a7dca80_0.geojson"
		gcontext = ssl.SSLContext()
		response = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r = json.loads(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		print("Success: [{}]".format(collection_name))

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
		print("Success: [{}]".format(collection_name))

		# MassGov
		collection_name = "massgov_most_recent_peak_hr"
		url = "http://datamechanics.io/data/ghonigsb_dezhouw/MostRecentPeakHrByYearVolume.csv"
		response = urllib.request.urlopen(url).read().decode("utf-8")
		fieldnames = ['local_id','dir','seven_to_eight','seven_to_nine','eleven_to_two','three_to_six','five_to_six','offpeak','daily','latitude','longitude','start_date','hpms_loc','daily1','aadt','on_road','approach','at_road']
		reader = csv.DictReader(io.StringIO(response), fieldnames)
		next(reader, None) # skip header
		repo.createCollection(collection_name)
		for row in reader:
			repo["dezhouw_ghonigsb."+collection_name].insert_one(row)
		print("Success: [{}]".format(collection_name))

        # Disconnect database for data safety
		repo.logout()

		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

try:
	print(flood.execute())
except Exception as e:
	traceback.print_exc(file = sys.stdout)
finally:
	print("Safely close")
