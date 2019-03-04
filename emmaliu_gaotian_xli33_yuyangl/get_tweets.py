#!/usr/bin/python
from twitter import *
import dml
import csv
import time
import json
import pymongo
#
# # MongoDB connection
# try:
#     mongodb = pymongo.MongoClient('mongodb://localhost')
#     mongo = mongodb['Twitter_API']
# except Exception as e:
#     print('Error: Cannot connect to MongoDB! Please check the configuration.')
#     raise e
# print('Successfully connected to MongoDB!')
# print('------------------------------------')

latitude = 31.947
longitude = 35.925
max_range = 80
num_results = 5000
# outfile = "output.csv"

data = []

# create twitter API object
twitter = Twitter(
    auth=OAuth(dml.auth['services']['Access']['Access_token'], dml.auth['services']['Access']['Access_token_secret'],
               dml.auth['services']['Consumer']['API_key'], dml.auth['services']['Consumer']['API_secret_key']))

# # open a file to write (mode "w"), and create a CSV writer object
# csvfile = open(outfile, 'w', encoding='utf-8')
# csvwriter = csv.writer(csvfile)
#
# # add headings to our CSV file
# row = ["Username", "Profile URL", "Latitude", "Longitude", "Tweet"]
# csvwriter.writerow(row)
with open('tweets_amman.json', 'w') as outfile:
    result_count = 0
    last_id = None
    while result_count < num_results:
        # perform a search based on latitude and longitude
        query = twitter.search.tweets(q="", geocode="%f,%f,%dkm" % (latitude, longitude, max_range),
                                      num_results=100, max_id=last_id, count=100)
        for result in query["statuses"]:
            print(result)
            data.append(result)
            # json.dump(result, outfile, indent=4)
            # only process a result if it has a geolocation
            # if result["geo"]:
            # user = result["user"]["screen_name"]
            # text = result["text"]
            # # text = text.encode('ascii', 'replace')
            # if result["geo"]:
            #     latitudes = result["geo"]["coordinates"][0]
            #     longitudes = result["geo"]["coordinates"][1]
            # else:
            #     latitudes = ''
            #     longitudes = ''
            # url = 'https://twitter.com/%s' % user
            # # gurl = 'https://maps.google.com/?q=' + str(latitude) + ',' + str(longitude)

            # now write this row to our CSV file
            # row = [user, url, latitudes, longitudes, str(text)]
            # print('-----------------------------------------------------------------')
            # print(' ')
            # print('Username:    ' + str(user))
            # print('Profile URL: ' + str(url))
            # print('Latitude:    ' + str(latitudes))
            # print('Longitude:   ' + str(longitudes))
            # print('Tweet:       ' + str(text))
            # print(' ')
            # csvwriter.writerow(row)
            result_count += 1
            # print(result["id"])
            print(result_count)
            # time.sleep(2)
            last_id = result["id"]
        time.sleep(2.5)
    json.dump(data, outfile, indent=4)
print("Got %d results" % result_count)
# csvfile.close()
