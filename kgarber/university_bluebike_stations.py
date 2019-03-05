import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from tqdm import tqdm

class university_bluebike_stations(dml.Algorithm):
    contributor = 'kgarber'
    reads = ['kgarber.bluebikes.stations', 'kgarber.university']
    writes = ['kgarber.university_bluebike_stations']
    @staticmethod
    def execute(trial = False):
        print("Starting university_bluebike_stations algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')

        repo.dropCollection("university_bluebike_stations")
        repo.createCollection("university_bluebike_stations")

        # describe the aggregation
        # get the universities with more than 500 students
        # find all bluebike stations within 1 mile of the university
        # run the aggregation in mongodb
        print("Finding universities of significant size.")
        repo['kgarber.university'].aggregate([
            {"$match": {"properties.NumStudent": {"$gt": 500}}},
            {"$out": "kgarber.university_bluebike_stations"}
        ])

        print("Finding nearest bike stations to each university")
        num_unis = 0
        for uni in repo['kgarber.university_bluebike_stations'].find():
            num_unis += 1
            nearest = repo['kgarber.bluebikes.stations'].find({"location.geometry": 
                {"$near": {
                    "$geometry": uni["geometry"],
                    "$maxDistance": 500  # 1/2 km. 1600m = 1 mile
                }}
            })
            nearest_stations = [n["stationId"] for n in nearest]
            repo['kgarber.university_bluebike_stations'].update(
                {"properties.Name": uni["properties"]["Name"]},
                {"$set":{"nearbyStations": nearest_stations}}
            )

        # For each university, find bluebike rides on monday through friday
        # which arrive at a station of the university between 7 and 11 AM. Count 
        # the most frequent stations to depart from to get to the university
        print("Finding most common departure location for getting to each university.")
        unis = [uni for uni in repo['kgarber.university_bluebike_stations'].find()]
        for uni in tqdm(unis):
            common_stations = repo['kgarber.bluebikes'].aggregate([
                {"$addFields": {
                    "startHour": {"$hour": {"$toDate": "$starttime"}},
                    "startDay": {"$dayOfWeek": {"$toDate": "$starttime"}}
                }},
                {"$match": {
                    "end station id": {"$in": uni["nearbyStations"]},
                    "startHour": {"$gte": 7, "$lt": 11},
                    "startDay": {"$gte": 2, "$lte": 6}
                }},
                {"$group": {
                    "_id": "$start station id",
                    "stationName": {"$first": "$start station name"},
                    "stationCount": {"$sum": 1}
                }},
                {"$sort": {"stationCount": -1}},
                {"$limit": 10}
            ])
            common = [cs for cs in common_stations]
            repo['kgarber.university_bluebike_stations'].update(
                {"properties.Name": uni["properties"]["Name"]},
                {"$set":{"commonStations": common}}
            )

        # indicate that the collection is complete
        repo['kgarber.university_bluebike_stations'].metadata({'complete':True})
        
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished university_bluebike_stations algorithm.")
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

# university_bluebike_stations.execute()
