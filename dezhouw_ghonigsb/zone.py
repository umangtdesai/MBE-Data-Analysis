# zone.py
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

class zone(dml.Algorithm):
	contributor = "dezhouw_ghonigsb"
	reads       = []
	writes      = [
				   "zoning_subdistricts",
				   "zillow_boston_neighborhood"]

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo   = client.repo
		repo.authenticate("dezhouw_ghonigsb", "dezhouw_ghonigsb")

		# Drop all collections
		collection_names = repo.list_collection_names()
		for collection_name in collection_names:
			repo.dropCollection(collection_name)


		# zoning_subdistricts
		collection_name = "zoning_subdistricts"
		url             = "https://opendata.arcgis.com/datasets/b601516d0af44d1c9c7695571a7dca80_0.geojson"
		gcontext        = ssl.SSLContext()
		response        = urllib.request.urlopen(url, context=gcontext).read().decode("utf-8")
		r               = json.loads(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		print("Success: [{}]".format(collection_name))

		# census
		collection_name = "zillow_boston_neighborhood"
		params          = {
			'zws-id'   : dml.auth["census"]["Zillow API"]["zws-id"],
			'state'    : 'ma',
			'city'     : 'boston',
			'childtype': 'neighborhood'
		}
		args            = urllib.parse.urlencode(params).encode('UTF-8')
		url             = "https://www.zillow.com/webservice/GetRegionChildren.htm?"
		gcontext        = ssl.SSLContext()
		response        = urllib.request.urlopen(url, args, context=gcontext).read().decode("utf-8")
		r               = xmltodict.parse(response)
		repo.createCollection(collection_name)
		repo["dezhouw_ghonigsb."+collection_name].insert_one(r)
		print("Success: [{}]".format(collection_name))



        # Disconnect database for data safety
		repo.logout()

		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/')

		# extra resources
		doc.add_namespace('opd', 'https://opendata.arcgis.com/datasets/')
		doc.add_namespace('zil', 'https://www.zillow.com/webservice/')
		doc.add_namespace('gov', 'http://datamechanics.io/data/ghonigsb_dezhouw/')

		this_script   = doc.agent('alg:dezhouw_ghonigsb#flood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource1     = doc.entity('opd:boston', {'prov:label':'Opendata Website', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
		resource2     = doc.entity('zil:boston', {'prov:label':'Zillow API',       prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'xml'})
		resource3     = doc.entity('gov:boston', {'prov:label':'MassGov',          prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		get_seaAnnual = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_seaTide   = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_zoning    = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_zillow    = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_massGov   = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_zoning,    this_script)
		doc.wasAssociatedWith(get_zillow,    this_script)


		doc.usage(get_zillow,    resource2, startTime, None,
			      {prov.model.PROV_TYPE:'ont:Retrieval',
			      'ont:Query':'?type=Boston+Neighborhood'})


		zoning    = doc.entity('dat:dezhouw_ghonigsb#zoning_subdistricts',
								{prov.model.PROV_LABEL:'Zone Subdistricts',
								 prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(zoning, this_script)
		doc.wasGeneratedBy(zoning, get_zoning, endTime)
		doc.wasDerivedFrom(zoning, resource1, get_zoning, get_zoning, get_zoning)

		zillow    = doc.entity('dat:dezhouw_ghonigsb#zillow_boston_neighborhood',
								{prov.model.PROV_LABEL:'Boston Neighborhood',
								 prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(zillow, this_script)
		doc.wasGeneratedBy(zillow, get_zillow, endTime)
		doc.wasDerivedFrom(zillow, resource1, get_zillow, get_zillow, get_zillow)

		massGov   = doc.entity('dat:dezhouw_ghonigsb#massgov_most_recent_peak_hr',
								{prov.model.PROV_LABEL:'Peak Hours',
								 prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(massGov, this_script)
		doc.wasGeneratedBy(massGov, get_massGov, endTime)
		doc.wasDerivedFrom(massGov, resource1, get_massGov, get_massGov, get_massGov)

		return doc

if __name__ == '__main__':
	try:
		print(flood.execute())
		doc = flood.provenance()
		print(doc.get_provn())
		print(json.dumps(json.loads(doc.serialize()), indent=4))
	except Exception as e:
		traceback.print_exc(file = sys.stdout)
	finally:
		print("Safely close")
