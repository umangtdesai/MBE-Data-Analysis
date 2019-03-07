import pandas as pd 
import requests
import numpy as np 
import json
import urllib
import xmljson
from xmljson import badgerfish as bf
import lxml
import xml
import collections

df = pd.read_csv('C:/users/calla/Downloads/ChelseaAssessorsDatabase2018.csv')
print(df.head())
addresses = list(zip(df['Street Number'].values.tolist(), df['Street Name'].values.tolist()))

import xml.etree.ElementTree as ET
homes = []
for x in addresses[0:2]:
	street_num = x[0]
	street = x[1].replace(' ', '+')
	print(street)
	url = 'http://www.zillow.com/webservice/GetSearchResults.htm?zws-id=X1-ZWz1gy5i8q08i3_3l8b3&address=' + street_num + '+' + street + '&citystatezip=Chelsea%2C+MA'
	response = urllib.request.urlopen(url).read()
	response = xml.etree.ElementTree.fromstring(response)
	response = bf.data(response)
	response = json.dumps(response)
	result = json.loads(response, object_pairs_hook=collections.OrderedDict)
	print(result)