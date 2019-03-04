import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

# my imports
import csv
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile


# These files come from https://www.mbta.com/developers/gtfs and come as a zip archive
# - stops
# - lines
# - routes


class download_mbta_stops(dml.Algorithm):
    contributor = 'kgarber'
    reads = []
    writes = ['kgarber.mbta.stops', 'kgarber.mbta.lines', 'kgarber.mbta.routes']
    # fields to cast to int or float after downloading them, per each file
    stops_fields_to_float = ["stop_lat", "stop_lon"]

    @staticmethod
    def execute(trial = False):
        print("Starting download_mbta_stops algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("mbta.lines")
        repo.dropCollection("mbta.routes")
        repo.createCollection("mbta.lines")
        repo.createCollection("mbta.routes")

        url = "https://cdn.mbta.com/MBTA_GTFS.zip"
        zip_urlopen = urllib.request.urlopen(url)
        # read the zip archive
        with ZipFile(BytesIO(zip_urlopen.read())) as current_zip_file:
        	stops_csv, lines_csv, routes_csv = {}, {}, {}
        	# process all stops
        	print("processing stops.txt")
        	with current_zip_file.open("stops.txt") as stops_file:
        		stops_csv = csv.DictReader(TextIOWrapper(stops_file))
        		stops_result = []
	        	for stop in stops_csv:
	        		this_stop = {field: stop[field] for field in stop}
	        		for field in download_mbta_stops.stops_fields_to_float:
	        			this_stop[field] = float(this_stop[field])
	        		stops_result.append(this_stop)
        		repo.dropCollection("mbta.stops")
        		repo.createCollection("mbta.stops")
        		repo['kgarber.mbta.stops'].insert_many(stops_result)
        		repo['kgarber.mbta.stops'].metadata({'complete':True})
	       	print("processing lines.txt")
        	with current_zip_file.open("lines.txt") as lines_file:
        		lines_csv = csv.DictReader(TextIOWrapper(lines_file))
        		lines_result = []
	        	for line in lines_csv:
	        		this_line = {field: line[field] for field in line}
        			lines_result.append(this_line)
        		repo.dropCollection("mbta.lines")
        		repo.createCollection("mbta.lines")
        		repo['kgarber.mbta.lines'].insert_many(lines_result)
        		repo['kgarber.mbta.lines'].metadata({'complete':True})
	        print("processing routes.txt")
        	with current_zip_file.open("routes.txt") as routes_file:
        		routes_csv = csv.DictReader(TextIOWrapper(routes_file))
        		routes_result = []
        		for route in routes_csv:
        			this_route = {field: route[field] for field in route}
        			routes_result.append(this_route)
        		repo.dropCollection("mbta.routes")
        		repo.createCollection("mbta.routes")
        		repo['kgarber.mbta.routes'].insert_many(routes_result)
        		repo['kgarber.mbta.routes'].metadata({'complete':True})
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished download_mbta_stops algorithm.")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # our data mechanics class namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # the namespace for geospatial datasets in boston data portal
        doc.add_namespace('blb', 'https://www.bluebikes.com/system-data')

        # the agent which is my algorithn
        this_script = doc.agent(
            'alg:kgarber#download_bluebikes', 
            {
                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 
                'ont:Extension':'py'
            })
        # the entity I am downloading
        resource = doc.entity(
            'blb:data2018',
            {
                'prov:label':'Bluebikes Dataset', 
                prov.model.PROV_TYPE:'ont:DataResource', 
                'ont:Extension':'csv'
            })
        # the activity of downloading this dataset (log the timing)
        get_bluebikes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # the activity is associated with the agent
        doc.wasAssociatedWith(get_bluebikes, this_script)
        # log an invocation of the activity
        doc.usage(get_bluebikes, resource, startTime, None,
            {
                prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'https://s3.amazonaws.com/hubway-data/index.html'
            })
        # the newly generated entity
        bluebikes = doc.entity(
            'dat:kgarber#bluebikes', 
            {
                prov.model.PROV_LABEL:'Bluebikes', 
                prov.model.PROV_TYPE:'ont:DataSet'
            })
        # relations for the above entity
        doc.wasAttributedTo(bluebikes, this_script)
        doc.wasGeneratedBy(bluebikes, get_bluebikes, endTime)
        doc.wasDerivedFrom(bluebikes, resource, get_bluebikes, get_bluebikes, get_bluebikes)
        
        # return the generated provenance document
        return doc
