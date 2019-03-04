import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class health_uber_output(dml.Algorithm):
    contributor = 'chuci_yfch_yuwan_zhurh'
    reads = ['chuci_yfch_yuwan_zhurh.uber', 'chuci_yfch_yuwan_zhurh.health']
    writes = ['chuci_yfch_yuwan_zhurh.population', 'chuci_yfch_yuwan_zhurh.uber_mean', 'chuci_yfch_yuwan_zhurh.city_pop_time']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        # Get the uber dataset
        client = dml.pymongo.MongoClient()
        repo1 = client.repo1
        print('AAAAAAAAAAAAA')
        repo1.authenticate('chuci_yfch_yuwan_zhurh', 'chuci_yfch_yuwan_zhurh')
        # Aggreate cityname by max population
        print('BBBBBBBBBBBBB')
        pipline = [
            {'$group': {'_id': "$CityName", 'population': {'$max': "$PopulationCount"}}}
        ]
        agg_result = repo1['chuci_yfch_yuwan_zhurh.health'].aggregate(pipline)
        agg_list = list(agg_result)
        repo1.dropCollection("population")
        repo1.createCollection("population")
        repo1['chuci_yfch_yuwan_zhurh.population'].insert_many(agg_list)

        # Aggreate cityname by mean time

        pipline2 = [
            {'$group': {'_id': "$CITY", 'mean_time': {'$avg': "$Mean Travel Time (Seconds)"}}}
        ]
        agg_result_uber = repo1['chuci_yfch_yuwan_zhurh.uber'].aggregate(pipline2)
        agg_list_uber = list(agg_result_uber)
        repo1.dropCollection("uber_mean")
        repo1.createCollection("uber_mean")
        repo1['chuci_yfch_yuwan_zhurh.uber_mean'].insert_many(agg_list_uber)

        # use basic relation to join population and mean

        l1 = list(repo1['chuci_yfch_yuwan_zhurh.population'].find())

        l2 = list(repo1['chuci_yfch_yuwan_zhurh.uber_mean'].find())

        l3 = []
        for each1 in l1:
            for each2 in l2:
                if each1['_id'] == each2['_id']:
                    l3.append({'_id': each1['_id'],
                               'population': each1['population'],
                               'mean_time': each2['mean_time']
                               })
        repo1.dropCollection("city_pop_time")
        repo1.createCollection("city_pop_time")
        repo1['chuci_yfch_yuwan_zhurh.city_pop_time'].insert_many(l3)

        repo1.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chuci_yfch_yuwan_zhurh', 'chuci_yfch_yuwan_zhurh')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_found = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        lost = doc.entity('dat:alice_bob#lost',
                          {prov.model.PROV_LABEL: 'Animals Lost', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found',
                           {prov.model.PROV_LABEL: 'Animals Found', prov.model.PROV_TYPE: 'ont:DataSet'})
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