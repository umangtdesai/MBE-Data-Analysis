import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pymongo
from bson.objectid import ObjectId

class busRiders(dml.Algorithm):
	print("busRiders")
	contributor = 'ido_jconstan_jeansolo_suitcase'
	reads = ['ido_jconstan_jeansolo_suitcase.bu_transportation_study',
			 'ido_jconstan_jeansolo_suitcase.property_data']
	writes = ['ido_jconstan_jeansolo_suitcase.PropertyValueNonRiders']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')

		# create new things
		repo.dropCollection("PropertyValueNonRiders")
		repo.createCollection("PropertyValueNonRiders")

		#r = repo.ido_jconstan_jeansolo_suitcase.bu_transportation_study.find()
		# OBTAINING FIRST DATASET [Bu Transportation Study]
		url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/bu_transportation_study.json'
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		for i in r:
			i['Address'] = i['Address'].upper()

			if "STREET" in i['Address']:
			    i['Address'] = i['Address'].replace("STREET", "ST")
			if "PLACE" in i['Address']:
			    i['Address'] = i['Address'].replace("PLACE", "PL")
			if "TERRACE" in i['Address']:
			    i['Address'] = i['Address'].replace("TERRACE", "TER")
			if "AVENUE" in i['Address']:
			    i['Address'] = i['Address'].replace("AVENUE", "AVE")
			if "CIRCLE" in i['Address']:
			    i['Address'] = i['Address'].replace("CIRCLE", "CIR")
			if "COURT" in i['Address']:
			    i['Address'] = i['Address'].replace("COURT", "CT")
			if "LANE" in i['Address']:
			    i['Address'] = i['Address'].replace("LANE", "LN")
			if "ROAD" in i['Address']:
			    i['Address'] = i['Address'].replace("ROAD", "RD")
			if "PARK" in i['Address']:
			    i['Address'] = i['Address'].replace("PARK", "PK")

		#r1 = repo.ido_jconstan_jeansolo_suitcase.property_data.find()
		# OBTAINING SECOND DATA SET [Spark Property Data]
		url = 'http://datamechanics.io/data/ido_jconstan_jeansolo_suitcase/property_data.json'
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r1 = json.loads(response)
		for i in r1:
			i['Address 1'] = i['Address 1'].upper()

			if "STREET" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("STREET", "ST")
			if "PLACE" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("PLACE", "PL")
			if "TERRACE" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("TERRACE", "TER")
			if "AVENUE" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("AVENUE", "AVE")
			if "CIRCLE" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("CIRCLE", "CIR")
			if "COURT" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("COURT", "CT")
			if "LANE" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("LANE", "LN")
			if "ROAD" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("ROAD", "RD")
			if "PARK" in i['Address 1']:
			    i['Address 1'] = i['Address 1'].replace("PARK", "PK")

		# Transform 1
		# Get the students who do NOT ride the bus
		busRiders = []
		tempFlag = False
		for x in r1:
			for y in r:
			    # if the student does ride the bus
			    if x['Address 1'] in y['Address']:
			        busRiders.append(x)
		
		# Get the house price of students who do NOT ride the bus
		brHouseValue = []
		for x in busRiders:
		    brHouseValue.append({"Address 1": x['Address 1'], "Assessed Total":x['Assessed Total']})

		repo['ido_jconstan_jeansolo_suitcase.PropertyValueNonRiders'].insert_many(brHouseValue)

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

	#nonBusRiders.execute()