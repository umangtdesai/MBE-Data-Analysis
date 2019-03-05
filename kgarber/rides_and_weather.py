import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

# This algorithm creates 10 degree buckets from 0 to 100 and calculates the
# average day stats for each of those temperatures (average duration and 
# number of rides).

class rides_and_weather(dml.Algorithm):
    contributor = 'kgarber'
    reads = ['kgarber.bluebikes.rides_per_day', 'kgarber.weather']
    writes = [
        'kgarber.bluebikes.rides_and_weather',
        'kgarber.bluebikes.ride_weather_aggregate'
    ]
    @staticmethod
    def execute(trial = False):
        print("Starting rides_and_weather algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("bluebikes.rides_and_weather")
        repo.createCollection("bluebikes.rides_and_weather")
        repo.dropCollection("kgarber.bluebikes.ride_weather_aggregate")
        repo.createCollection("kgarber.bluebikes.ride_weather_aggregate")

        # describe the aggregation 
        # (applied to kgarber.bluebikes.rides_per_day)
        pipeline = [
            {"$lookup": 
                {
                    "from": "kgarber.weather",
                    "localField": "date",
                    "foreignField": "date",
                    "as": "weather"
                }
            },
            {"$project":
                {
                    "_id": 0, "date": 1, "numTrips": 1, "avgDuration": 1,
                    "tempMin": "$weather.tmin",
                    "tempMax": "$weather.tmax",
                    "tempAvg": "$weather.tavg"
                }
            },
            {"$unwind": "$tempMin"},
            {"$unwind": "$tempMax"},
            {"$unwind": "$tempAvg"},
            {"$project": 
                {
                    "date": 1, "numTrips": 1, "avgDuration": 1, "tempMin": 1, "tempMax": 1, "tempAvg": 1,
                    "tempMinBucket": {"$multiply": [{"$trunc": {"$divide": ["$tempMin", 10]}}, 10]},
                    "tempMaxBucket": {"$multiply": [{"$trunc": {"$divide": ["$tempMax", 10]}}, 10]},
                    "tempAvgBucket": {"$multiply": [{"$trunc": {"$divide": ["$tempAvg", 10]}}, 10]},
                }
            },
            {"$out": "kgarber.bluebikes.rides_and_weather"}
        ]
        # run the aggregation in mongodb
        repo['kgarber.bluebikes.rides_per_day'].aggregate(pipeline)
        # sort the rides into the buckets in a new collection
        pipeline2 = [
            {"$group": {
                "_id": "$tempAvgBucket",
                "avgTripsAtTemp": {"$avg": "$numTrips"},
                "avgTripDurationAtTemp": {"$avg": "$avgDuration"},
                "count": {"$sum": 1}
            }},
            {"$project": {
                "tempAvgBucket": "$_id",
                "_id": 0,
                "avgTripsAtTemp": {"$trunc": "$avgTripsAtTemp"},
                "avgTripDurationAtTemp": {"$trunc": "$avgTripDurationAtTemp"},
                "count": 1
            }},
            {"$sort": {"tempAvgBucket": 1}},
            {"$out": "kgarber.bluebikes.ride_weather_aggregate"}
        ]
        repo['kgarber.bluebikes.rides_and_weather'].aggregate(pipeline2)
        # indicate that the collection is complete
        repo['kgarber.bluebikes.rides_and_weather'].metadata({'complete':True})
        
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished rides_and_weather algorithm.")
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

# rides_and_weather.execute()
