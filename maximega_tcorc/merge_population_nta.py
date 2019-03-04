import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_population_nta(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.population', 'repo.maximega_tcorc.neighborhoods_with_stations']
	writes = ['maximega_tcorc.population_with_neighborhoods']
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		repo_name = merge_population_nta.writes[0]
		# ----------------- Set up the database connection -----------------
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')

		populations = repo.maximega_tcorc.population
		neighborhoods = repo.maximega_tcorc.neighborhoods_with_stations
		
		# ----------------- NTA info with NTA populations -----------------
		insert_many_arr = []
		for neighborhood in neighborhoods.find():
			for population in populations.find():
				if neighborhood['ntacode'] == population['nta_code']:
					insert_many_arr.append({
						'ntacode': neighborhood['ntacode'], 
						'ntaname': neighborhood['ntaname'], 
						'the_geom': neighborhood['the_geom'], 
						'stations': neighborhood['stations'], 
						'population': population['population']
					})

		#----------------- Data insertion into Mongodb ------------------
		repo.dropCollection('population_with_neighborhoods')
		repo.createCollection('population_with_neighborhoods')
		repo[repo_name].insert_many(insert_many_arr)
		repo[repo_name].metadata({'complete':True})
		print(repo[repo_name].metadata())

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
			Create the provenance document describing everything happening
			in this script. Each run of the script will generate a new
			document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alice_bob', 'alice_bob')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_found, this_script)
		doc.wasAssociatedWith(get_lost, this_script)
		doc.usage(get_found, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
					}
					)
		doc.usage(get_lost, resource, startTime, None,
					{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
					}
					)

		lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(lost, this_script)
		doc.wasGeneratedBy(lost, get_lost, endTime)
		doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

		found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_found, endTime)
		doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

		repo.logout()
					
		return doc
