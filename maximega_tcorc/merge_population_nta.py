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

		repo_name = merge_income.writes[0]
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
		print("")

merge_population_nta.execute()

