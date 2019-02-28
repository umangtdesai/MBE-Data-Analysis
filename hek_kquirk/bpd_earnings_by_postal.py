import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import bson.code

class bpd_earnings_by_postal(dml.Algorithm):
    contributor = 'hek_kquirk'
    reads = ['hek_kquirk.bpd_employee_earnings']
    writes = ['hek_kquirk.bpd_earnings_by_postal']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hek_kquirk', 'hek_kquirk')

        # Drop/recreate mongo collection
        repo.dropCollection("bpd_earnings_by_postal")
        repo.createCollection("bpd_earnings_by_postal")

        # Map to {postal, total earnings}
        mapper = bson.code.Code("""
           function() {
              var postal = this['POSTAL'];
              var total_earnings = parseFloat(this['TOTAL EARNINGS'].replace(/[$,]+/g,""));
              emit(postal, {total: total_earnings});
           }
        """)

        # Compute total for each postal
        reducer = bson.code.Code("""
           function(k, vs) {
              var total = 0;
              vs.forEach(function(v,i) {
                 total += v.total;
              });
              return {total};
           }
        """)

        # Run map reduce with our mapper and reducer
        repo['hek_kquirk.bpd_employee_earnings'].map_reduce(mapper, reducer, "hek_kquirk.bpd_earnings_by_postal")
        
        repo['hek_kquirk.bpd_earnings_by_postal'].metadata({'complete':True})
        print(repo['hek_kquirk.bpd_earnings_by_postal'].metadata())

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
