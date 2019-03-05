
import urllib.request
import requests
import json
import dml
import prov.model
import datetime
import uuid

class transformation1():
    contributor = 'kgrewal_shin2'
    reads = []
    writes = ['kgrewal_shin2.transformation1']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')

        landmarks = repo.kgrewal_shin2.landmarks.find()
        street_names = repo.kgrewal_shin2.street_names.find()

        landmark_streets = []
        for l in landmarks:
            length = len(l['features'])
            for i in range(length):
                data = l['features'][i]['properties']
                hood = data['Neighborho']
                address = data['Address']

                if "Bounded by" not in address:
                    street = address.split(" ", 1)
                    if street[0].isdigit() or len(street[0].split("-")) > 1:
                        street = street[1]
                    else:
                        street = address
                else:
                    street = address.split("Bounded by")[1]

                print(street)
                landmark_streets.append(street)

        streets_without_landmarks = []
        for x in street_names:
            full_name = x['full_name']
            gender = x['gender']
            zipcode = x['zipcodes']
            street_name = x['street_name']

            # find difference of streetnames and landmarks
            if full_name not in landmark_streets and gender != "female":
                streets_without_landmarks.append({"full_name": full_name, "gender": gender,
                                                  "zipcode": zipcode, "street_name": street_name})

        print(streets_without_landmarks)
        repo.dropCollection("streets_without_landmarks")
        repo.createCollection("streets_without_landmarks")
        repo['kgrewal_shin2.streets_without_landmarks'].insert_many(streets_without_landmarks)
        repo['kgrewal_shin2.streets_without_landmarks'].metadata({'complete': True})
        print(repo['kgrewal_shin2.streets_without_landmarks'].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}



    # @staticmethod
    # def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
    #     '''
    #         Create the provenance document describing everything happening
    #         in this script. Each run of the script will generate a new
    #         document describing that invocation event.
    #         '''
    #
    #     # Set up the database connection.
    #     client = dml.pymongo.MongoClient()
    #     repo = client.repo
    #     repo.authenticate('kgrewal_shin2', 'kgrewal_shin2')
    #     doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
    #     doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
    #     doc.add_namespace('ont',
    #                       'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    #     doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
    #     doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
    #

transformation1.execute()
# doc = ProvenanceModel.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

