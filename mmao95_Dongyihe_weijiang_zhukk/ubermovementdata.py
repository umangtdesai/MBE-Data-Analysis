import urllib.request
import json
import dml
import pymongo
import prov.model
import datetime
import uuid
import pandas as pd
import requests
from io import StringIO
import csv


class ubermovementdata(dml.Algorithm):
    contributor = 'mmao95_Dongyihe_weijiang_zhukk'
    reads = []
    writes = [contributor + '.uber_data']

    @staticmethod
    def execute(trial=False):
        start_time = datetime.datetime.now()
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        writes = [contributor + '.famous_people']

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        url = 'http://datamechanics.io/data/boston-censustracts-2018-4-All-MonthlyAggregate.csv'
        doc = requests.get(url).text
        data_file = StringIO(doc)
        reader = csv.reader(data_file)
        Uber_Movement_data_list = []
        for row in reader:
            Uber_Movement_data_list += [row]

        del Uber_Movement_data_list[0]

        # csv1 = pd.read_csv("/Users/zjallenjiang/Desktop/boston-censustracts-2018-4-All-MonthlyAggregate.csv").values.tolist()
        l1 = [(s, 1) for (s, a, b, c, d, e, f) in Uber_Movement_data_list]
        l2 = aggregate(l1, sum)
        columnName = ['Street ID', 'Count']
        df = pd.DataFrame(columns=columnName, data=l2)
        repo.dropCollection('uber_data')
        repo.createCollection('uber_data')
        data = json.loads(df.to_json(orient="records"))
        repo[writes[0]].insert_many(data)
        repo[writes[0]].metadata({'complete': True})
        repo.logout()
        endtime = datetime.datetime.now()
        return {"start": start_time, "end": endtime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        contributor = 'mmao95_Dongyihe_weijiang_zhukk'
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(contributor, contributor)

        # The scripts are in <folder>#<filename> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The data sets are in <user>#<collection> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # The event log.
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace(
            'bdp', 'https://movement.uber.com/explore/boston/travel-times/query?lang=en-US')

        this_script = doc.agent('alg:' + contributor + '#uber_people',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_names = doc.activity(
            'log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_names, this_script)
        doc.usage(get_names, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Computation': 'Data cleaning'
                   }
                  )

        fp = doc.entity('dat:' + contributor + '#uber_data',
                        {prov.model.PROV_LABEL: 'Uber Data', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fp, this_script)
        doc.wasGeneratedBy(fp, get_names, endTime)
        doc.wasDerivedFrom(fp, resource, get_names, get_names, get_names)

        repo.logout()

        return doc
