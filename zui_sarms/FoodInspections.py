"""
Pulling data from City of Boston data portal about
[Food Establishment Inspection](https://data.boston.gov/dataset/food-establishment-inspections)

# Resource ID: 4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

"""
import datetime
import tempfile

import dml
import pandas as pd
import prov.model

# Is this URL permanent?
from senpai.db import mongo_wrapper
from senpai.utils import download_file

RES_ID = "4582bec6-2b4f-4f9e-bc55-cbaa73117f4c"
URL = f"https://data.boston.gov/dataset/03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/{RES_ID}/download/tmp1yzpct9p.csv"


class FoodInspection(dml.Algorithm):
    contributor = 'zui_sarms'
    reads = []
    writes = ['zui_sarms.food_insp', 'zui_sarms.food_violations']

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

        

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        pass


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


def parse_coor(s):
    """
    Parse the string to tuple of coordinate
    In the format of (lat, long)
    """

    lat, long = s.split(", ")
    lat = lat[1:]
    long = long[:-1]
    lat = float(lat)
    long = float(long)

    return [lat, long]
