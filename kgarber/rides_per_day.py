import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

# my imports
import bson.code

# This algorithm calculates the number of bluebike rides per day in 2018.
# It uses a mongodb MapReduce approach.
# Every ride turns into a {date -> 1} pair, then we aggregate with a sum
# for all rides.

class rides_per_day(dml.Algorithm):
    contributor = 'kgarber'
    reads = ['kgarber.bluebikes']
    writes = ['kgarber.bluebikes.rides_per_day']
    @staticmethod
    def execute(trial = False):
        print("Starting rides_per_day algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("bluebikes.rides_per_day")
        repo.createCollection("bluebikes.rides_per_day")

        # emit {date -> 1}
        # date is in format "YYYY-MM-DD" (from starttime string)
        mapper = bson.code.Code("""
            function() {
                emit(this.starttime.slice(0,10), 1);
            }
        """)

        # functional javascript reduce code for summation
        reducer = bson.code.Code("""
            function(key, vs) {
                return vs.reduce((partialSum, x) => partialSum + x);
            }
        """)

        repo['kgarber.bluebikes'].map_reduce(
            mapper, 
            reducer, 
            "kgarber.bluebikes.rides_per_day"
        )

        # indicate that the collection is complete
        repo['kgarber.bluebikes.rides_per_day'].metadata({'complete':True})
        
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished rides_per_day algorithm.")
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

rides_per_day.execute()
