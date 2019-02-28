import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class stations_with_neighborhoods(dml.Algorithm):
    contributor = 'maximega_tcorc'
    repo_name = contributor + '.EXAMPLE'
    reads = []
    writes = ['maximega_tcorc.neighborhoods', 'maximega_tcorc.stations']

    def map(f, R):
        return [t for (k,v) in R for t in f(k,v)]
    
    def reduce(f, R):
        keys = {k for (k,v) in R}
        return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
       
        #TODO: change this later to be better (try to get it global)
        repo_name_neighborhood = 'maximega_tcorc.neighborhoods'
        repo_name_station = 'maximega_tcorc.stations'
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('maximega_tcorc', 'maximega_tcorc')

        url_neighborhood = 'https://data.cityofnewyork.us/resource/q2z5-ai38.json'
        # ----------------- API ACCESS KEYS & INFO ----------------
        auth = json.load(open('../auth.json', 'r+'))['services']['data.cityofnewyorkportal']
        token_key = auth['key']
        token_value = auth['token']
        req_neighborhood = urllib.request.Request(url_neighborhood)
        req_neighborhood.add_header(token_key, token_value)
        resp_neighborhood = urllib.request.urlopen(req_neighborhood)
        content_neighborhood = resp_neighborhood.read()
        r_neighborhood = json.loads(content_neighborhood)
        #project = map(lambda k,v: [(v[0], v[1])], r)
        s_neighborhood = json.dumps(r_neighborhood, sort_keys=True, indent=2)
    
        
        url_station = 'https://data.ny.gov/resource/hvwh-qtfg.json'
        req_station = urllib.request.Request(url_station)
        req_station.add_header(token_key, token_value)
        resp_station = urllib.request.urlopen(req_station)
        content_station = resp_station.read()
        r_station = json.loads(content_station)
        #project = map(lambda k,v: [(v[0], v[1])], r)
        s_station = json.dumps(r_station, sort_keys=True, indent=2)

        repo.dropCollection("neighborhoods")
        repo.createCollection("neighborhoods")
        repo[repo_name_neighborhood].insert_many(r_neighborhood)
        repo[repo_name_neighborhood].metadata({'complete':True})
        print(repo[repo_name_neighborhood].metadata())

        repo.dropCollection("stations")
        repo.createCollection("stations")
        repo[repo_name_station].insert_many(r_station)
        repo[repo_name_station].metadata({'complete':True})
        print(repo[repo_name_station].metadata())
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        print()

stations_with_neighborhoods.execute()