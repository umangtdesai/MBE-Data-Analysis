import datetime
import tempfile
import uuid

import dml
import pandas as pd
import prov.model

# Is this URL permanent?
from senpai.db import mongo_wrapper
from senpai.utils import download_file, parse_coor


class FoodViolations(dml.Algorithm):
    contributor = 'zui_sarms'
    reads = ["zui_sarms.food_inspections"]
    writes = ['zui_sarms.food_violations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        # This will fail to connect to the one require SSH auth
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('zui_sarms', 'zui_sarms')

        df = pd.DataFrame(list(repo["zui_sarms.food_inspections"].find()))

        # Project to select only the column we wants
        selected_columns = ["businessname", "licenseno", "violstatus", "address", "city", "state", "zip", "property_id",
                            "location"]
        DF = df[selected_columns]

        # Remove the row which has an empty value [x for row in table for x in row if x]
        DF = DF.dropna()

        # Select all the Fail violations
        DF = DF[DF["violstatus"] == "Fail"]

        # Count violations per restaurant
        VC = DF.groupby("licenseno").count()

        DF_R = DF.set_index("licenseno")[["businessname", "address", "city", "state", "zip", "location"]]
        DF_R["violation_count"] = VC["violstatus"]
        DF_R = DF_R.drop_duplicates()

        DF_R["_id"] = DF_R.index.values
        DF_R["location"] = DF_R["location"].map(parse_coor)

        r_dict = DF_R.to_dict(orient="record")

        repo.dropCollection("food_violations")
        repo.createCollection("food_violations")
        repo['zui_sarms.food_violations'].insert_many(r_dict)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#FoodViolations',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:4582bec6-2b4f-4f9e-bc55-cbaa73117f4c',
                              {'prov:label': 'Food Establishment Violation', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'csv'})
        get_fi = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_fi, this_script)
        doc.usage(get_fi, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:DataSet': '03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/{RES_ID}/download/tmp1yzpct9p.csv'
                   }
                  )
        fi = doc.entity('dat:zui_sarms#FoodInspection',
                        {prov.model.PROV_LABEL: 'Food Inspections', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fi, this_script)
        doc.wasGeneratedBy(fi, get_fi, endTime)
        doc.wasDerivedFrom(fi, resource, get_fi, get_fi, get_fi)

        repo.logout()

        return doc
