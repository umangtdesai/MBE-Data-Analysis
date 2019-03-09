"""
Pulling data from City of Boston data portal about
[Food Establishment Inspection](https://data.boston.gov/dataset/food-establishment-inspections)

# Resource ID: 4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

"""
import datetime
import tempfile
import uuid

import dml
import pandas as pd
import prov.model

from senpai.utils import download_file

RES_ID = "4582bec6-2b4f-4f9e-bc55-cbaa73117f4c"
# Is this URL permanent?
URL = f"https://data.boston.gov/dataset/03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/{RES_ID}/download/tmp1yzpct9p.csv"


class FoodInspections(dml.Algorithm):
    contributor = 'zui_sarms'
    reads = []
    writes = ['zui_sarms.food_inspections']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        # Set up the database connection.
        # This will fail to connect to the one require SSH auth
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('zui_sarms', 'zui_sarms')

        df = download_csv(URL)

        # Project to select only the column we wants
        selected_columns = ["businessname", "licenseno", "violstatus", "address", "city", "state", "zip", "property_id",
                            "location"]
        DF = df[selected_columns]

        # Remove the row which has an empty value [x for row in table for x in row if x]
        DF = DF.dropna()

        DF["_id"] = DF.index.values

        r_dict = DF.to_dict(orient="record")

        repo.dropCollection("food_inspections")
        repo.createCollection("food_inspections")
        repo['zui_sarms.food_inspections'].insert_many(r_dict)

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

        this_script = doc.agent('alg:alice_bob#FoodInspection',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:4582bec6-2b4f-4f9e-bc55-cbaa73117f4c',
                              {'prov:label': 'Food Establishment Inspections', prov.model.PROV_TYPE: 'ont:DataResource',
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



def download_csv(url):
    """
    Download the file at the URL

    :param str url:
    :return: pandas.DataFrame containing the content of the CSV file
    :rtype: pandas.DataFrame
    """
    temp = tempfile.TemporaryFile()
    temp.write(download_file(url))

    # After we finished writing the temp-file, we can read it as a CSV file

    df = pd.read_csv(temp)

    temp.close()

    return df

