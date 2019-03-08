"""
Pulling data from City of Boston data portal about
[Food Establishment Inspection](https://data.boston.gov/dataset/food-establishment-inspections)

# Resource ID: 4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

"""
import tempfile
import pandas as pd

from senpai.db import mongo_wrapper

# Is this URL permanent?
from senpai.utils import download_file

RES_ID = "4582bec6-2b4f-4f9e-bc55-cbaa73117f4c"
URL = f"https://data.boston.gov/dataset/03693648-2c62-4a2c-a4ec-48de2ee14e18/resource/{RES_ID}/download/tmpjjcw1r_i.csv"
@mongo_wrapper
def insert_all(data, db=None):
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


