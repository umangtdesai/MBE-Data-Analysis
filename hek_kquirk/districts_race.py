import geopandas
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import bson.code

class districts_race(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.race_and_ethnicity', 'hek_kquirk.neighborhood_district']
    writes = ['hek_kquirk.district_race']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        repo.dropCollection("hek_kquirk.district_race")
        repo.createCollection("hek_kquirk.district_race")
        repo.dropCollection("hek_kquirk.tmp_collection")
        repo.createCollection("hek_kquirk.tmp_collection")

        pipeline = [
            {"$lookup": {"from":"hek_kquirk.neighborhood_district", "localField": "Neighborhood", "foreignField": "_id", "as": "tmp"}},
            {"$project":{"District":"$tmp.district", "_id":"$Neighborhood", "Total_Population":"$Total Population", "Non_Hispanic_White":"$Non-Hispanic White", "Non_Hispanic_Black_African_American":"$Non-Hispanic Black/African-American", "Hispanic_or_Latino":"$Hispanic or Latino", "Non_Hispanic_Asian": "$Non-Hispanic Asian", "Other_Races_or_multiple_races": "$Other Races or multiple races"}},
            {"$out":"hek_kquirk.tmp_collection"}
        ]

        repo['hek_kquirk.race_and_ethnicity'].aggregate(pipeline)

        '''
        repo['hek_kquirk.race_and_ethnicity'].aggregate([{$lookup: {from:"hek_kquirk.neighborhood_district", localField: "Neighborhood", foreignField: "_id", as: "tmp"}}, {$project:{"District":"$tmp.district", "_id":"Neighborhood", "Total_Population":"$Total Population", "Non-Hispanic_White":"$Non-Hispanic White", "Non-Hispanic_Black/African-American":"$Non-Hispanic Black/African-American", "Hispanic_or_Latino":"$Hispanic or Latino", "Non-Hispanic_Asian": "$Non-Hispanic Asian", "Other_Races_or_multiple_races": "$Other Races or multiple races"}}])
        '''

        mapper = bson.code.Code("""
            function() {
                var stats = {'Total_Population': this.Total_Population, 'Non_Hispanic_White': this.Non_Hispanic_White, 'Non_Hispanic_Black_African_American': this.Non_Hispanic_Black_African_American, 'Hispanic_or_Latino': this.Hispanic_or_Latino, 'Other_Races_or_multiple_races': this.Other_Races_or_multiple_races};
                emit(this.District[0], stats);
            }
        """)         

        reducer = bson.code.Code("""
            function(k, vs) {
                var tot_p = 0;
                var tot_nhw = 0;
                var tot_nhb = 0;
                var tot_hol = 0;
                var tot_othr = 0;

                vs.forEach(function(v,i) {
                    tot_p += parseFloat(String(v.Total_Population).replace(/[$,\(\)]+/g,""));
                    tot_nhw += parseFloat(String(v.Non_Hispanic_White).replace(/[$,\(\)]+/g,""));
                    tot_nhb += parseFloat(String(v.Non_Hispanic_Black_African_American).replace(/[$,\(\)]+/g,""));
                    tot_hol += parseFloat(String(v.Hispanic_or_Latino).replace(/[$,\(\)]+/g,""));
                    tot_othr += parseFloat(String(v.Other_Races_or_multiple_races).replace(/[$,\(\)]+/g,""));
                });
                return {'Total_Population':tot_p, 'Non_Hispanic_White': tot_nhw, 'Non_Hispanic_Black_African_American': tot_nhb, 'Hispanic_or_Latino': tot_hol, 'Other_Races_or_multiple_races':tot_othr};
            }
        """)

        repo['hek_kquirk.tmp_collection'].map_reduce(mapper, reducer, "hek_kquirk.district_race")
        repo['hek_kquirk.district_race'].metadata({'complete':True})
        print(repo['hek_kquirk.district_race'].metadata())

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

'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
