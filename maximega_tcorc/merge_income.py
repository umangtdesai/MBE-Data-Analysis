import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class merge_income(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.income_with_tracts', 'repo.maximega_tcorc.population_with_neighborhoods']
	writes = ['maximega_tcorc.income_with_neighborhoods']
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		repo_name = merge_income.writes[0]
		# ----------------- Set up the database connection -----------------
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        # ----------------- Retrieve data from Mongodb -----------------
		incomes = repo.maximega_tcorc.income_with_tracts
		neighborhoods = repo.maximega_tcorc.population_with_neighborhoods
		
		# ----------------- Merge Census Tract incomes with NTA info, aggregate and average incomes for NTA -----------------
		insert_many_arr = []
		for neighborhood in neighborhoods.find():
			for income in incomes.find():
				if neighborhood['ntacode'] == income['nta']:
					insert_many_arr.append({
						'ntacode': neighborhood['ntacode'], 
						'ntaname': neighborhood['ntaname'],  
						'stations': neighborhood['stations'], 
						'population': neighborhood['population'],
						'income': income['income']
					})

		#----------------- Data insertion into Mongodb ------------------
		repo.dropCollection('income_with_neighborhoods')
		repo.createCollection('income_with_neighborhoods')
		repo[repo_name].insert_many(insert_many_arr)
		repo[repo_name].metadata({'complete':True})
		print(repo[repo_name].metadata())

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		print("")

merge_income.execute()

