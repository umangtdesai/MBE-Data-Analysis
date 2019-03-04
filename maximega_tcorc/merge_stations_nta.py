import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

def point_inside_polygon(x,y,poly):
	#function from:
	#http://www.ariel.com.au/a/python-point-int-poly.html

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

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('maximega_tcorc', 'maximega_tcorc')
		repo_name = merge_stations_nta.writes[0]

		stations = repo.maximega_tcorc.stations.find()
		ntas = repo.maximega_tcorc.neighborhoods.find()


		duplicates = []
		filtered_stations = []

		for station in stations:
			name = station['station_name']
			if name not in duplicates:
				duplicates.append(name)
				filtered_stations.append(station)

		nta_objects = {}

		nta_count = 0
		station_count = 0

		for nta in ntas:

			nta_objects[nta['ntacode']] = {'ntacode': nta['ntacode'],'ntaname': nta['ntaname'], 'the_geom': nta['the_geom'], 'stations': []}

			nta_multipolygon = nta['the_geom']['coordinates'][0][0]

			for station in filtered_stations:

				station_coordinates = station['entrance_location']['coordinates']
				is_in_nta = point_inside_polygon(station_coordinates[0], station_coordinates[1], nta_multipolygon)

				if is_in_nta:
					nta_objects[nta['ntacode']]['stations'].append({
						'station_name': station['station_name']
					})

		insert_many_arr = []

		for key in nta_objects.keys():
			insert_many_arr.append(nta_objects[key])

		print("insert_many_arr", len(insert_many_arr))

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
		print("")

merge_stations_nta.execute()

