import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

# ----------------- function source: http://www.ariel.com.au/a/python-point-int-poly.html -----------------
def point_inside_polygon(x,y,poly):
	n = len(poly)
	inside =False

	p1x,p1y = poly[0]
	for i in range(n+1):
	    p2x,p2y = poly[i % n]
	    if y > min(p1y,p2y):
	        if y <= max(p1y,p2y):
	            if x <= max(p1x,p2x):
	                if p1y != p2y:
	                    xinters = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
	                if p1x == p2x or x <= xinters:
	                    inside = not inside
	    p1x,p1y = p2x,p2y
	return inside

class merge_stations_nta(dml.Algorithm):
	contributor = 'maximega_tcorc'
	reads = ['maximega_tcorc.stations', 'maximega_tcorc.neighborhoods']
	writes = ['maximega_tcorc.neighborhoods_with_stations']
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		repo_name = merge_stations_nta.writes[0]
		# ----------------- Set up the database connection -----------------
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')

		stations = repo.maximega_tcorc.stations.find()
		ntas = repo.maximega_tcorc.neighborhoods.find()

		duplicates = []
		filtered_stations = []
		# ----------------- Create a list of unique stations (stations data contains many duplicates) -----------------
		for station in stations:
			name = station['Station Name']
			if name not in duplicates:
				duplicates.append(name)
				filtered_stations.append(station)

		nta_objects = {}
		nta_count = 0
		station_count = 0
		# ----------------- Merge NTA info with station info if the subway station is inside of NTA multi polygon -----------------
		for nta in ntas:
			nta_objects[nta['ntacode']] = {'ntacode': nta['ntacode'],'ntaname': nta['ntaname'], 'the_geom': nta['the_geom'], 'stations': []}
			nta_multipolygon = nta['the_geom']['coordinates'][0][0]

			for station in filtered_stations:
				# ----------------- station coordinates come in form: (lat, long) as a string -----------------
				# ----------------- retreive lat and long points, cast them to floats to be passed into point_inside_polygon function -----------------
				station_coordinates = station['Entrance Location']
				lat_coord = ''
				long_coord = ''
				i = 1
				while(station_coordinates[i] != ','):
					lat_coord += station_coordinates[i]
					i += 1
				i += 2
				while(station_coordinates[i] != ')'):
					long_coord += station_coordinates[i]
					i += 1
				lat_coord = float(lat_coord)
				long_coord = float(long_coord)
				#print(type(nta_multipolygon[0][0]))
				is_in_nta = point_inside_polygon(long_coord, lat_coord, nta_multipolygon)
				if is_in_nta:
					nta_objects[nta['ntacode']]['stations'].append({
						'station_name': station['Station Name']
					})
		# ----------------- Reformat data for mongodb insertion -----------------
		insert_many_arr = []
		for key in nta_objects.keys():
			insert_many_arr.append(nta_objects[key])

		#----------------- Data insertion into Mongodb ------------------
		repo.dropCollection('neighborhoods_with_stations')
		repo.createCollection('neighborhoods_with_stations')
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


