import dml
import prov.model
import datetime
import uuid
from ldisalvo_skeesara_vidyaap.helper.constants import TEAM_NAME, STATE_HOUSE_ELECTIONS_NAME, STATE_HOUSE_ELECTIONS_RESULTS_NAME, WEIGHTED_HOUSE_IDEOLOGIES_NAME, WEIGHTED_HOUSE_IDEOLOGIES


class transformationWeightedHouseIdeology(dml.Algorithm):
    contributor = TEAM_NAME
    reads = [STATE_HOUSE_ELECTIONS_NAME, STATE_HOUSE_ELECTIONS_RESULTS_NAME]
    writes = [WEIGHTED_HOUSE_IDEOLOGIES_NAME]

    @staticmethod
    def execute(trial=False):
        """

        """
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(TEAM_NAME, TEAM_NAME)


        electionsByDistrict = list(repo[STATE_HOUSE_ELECTIONS_NAME].find({}))
        finalAvgsByDistrict = {}


        for tup in electionsByDistrict:
            # find the results row with the district totals
            totalRow = list(repo[STATE_HOUSE_ELECTIONS_RESULTS_NAME].find(
                {"City/Town":"TOTALS",
                 "Election ID":tup["_id"]
                 }))[0]


            # find which candidate was the Democrat and which was the Republican
            dem = ""
            rep = ""
            others = []

            for c in tup["candidates"]:
                candidate = c["party"]
                if candidate == "Democratic":
                    dem = c["name"].replace('.','')
                elif candidate == "Republican":
                    rep = c["name"].replace('.','')
                else:
                    others += [c["name"].replace('.', '')]

            # find the ratio of votes that each candidate got

            total_count = totalRow["Total Votes Cast"]
            dem_ratio = 0
            rep_ratio = 0
            others_count = 0

            for o in others:
                if o != "":
                    try:
                        others_count += totalRow[o]
                    except KeyError:
                        print("unable to find 'other' candidate -->", o)

            others_ratio = float(others_count/total_count)
            blanks_ratio = float(totalRow["Blanks"]/total_count)

            if dem != "":
                try:
                    dem_ratio = float(totalRow[dem]/total_count)
                except KeyError:
                    print("unable to find dem = ", dem)


            if rep != "":
                try:
                    rep_ratio = float(totalRow[rep]/total_count)
                except KeyError:
                    print("unable to find rep = ", rep)


            if tup["district"] in finalAvgsByDistrict:
                finalAvgsByDistrict[tup["district"]] += [(dem_ratio, rep_ratio, others_ratio, blanks_ratio)]
            else:
                finalAvgsByDistrict[tup["district"]] = [(dem_ratio, rep_ratio, others_ratio, blanks_ratio)]


        new_list = []
        for k, v in finalAvgsByDistrict.items():
            avg_tup = transformationWeightedHouseIdeology.tuple_avg(v)
            new_json = {}
            new_json["district"] = k
            new_json["Democratic ratio"] = avg_tup[0]
            new_json["Republican ratio"] = avg_tup[1]
            new_json["Others ratio"] = avg_tup[2]
            new_json["Blanks ratio"] = avg_tup[3]
            new_json["Total"] = avg_tup[4]
            new_list += [new_json]


        repo.dropCollection(WEIGHTED_HOUSE_IDEOLOGIES)
        repo.createCollection(WEIGHTED_HOUSE_IDEOLOGIES_NAME)
        repo[WEIGHTED_HOUSE_IDEOLOGIES_NAME].insert_many(new_list)
        repo[WEIGHTED_HOUSE_IDEOLOGIES_NAME].metadata({'complete': True})
        print(repo[WEIGHTED_HOUSE_IDEOLOGIES_NAME].metadata())


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def tuple_avg(tupList):

        # print("tupList = ", tupList)

        count = 0
        tupSum = [0]*len(tupList[0])

        for tup in tupList:
            count += 1
            for i in range(len(tup)):
                tupSum[i] += tup[i]

        # print("tupSum = ", tupSum)

        tupAvg = [0]*len(tupSum)

        for i in range(len(tupSum)):
            tupAvg[i] = tupSum[i]/count

        total = sum(tupAvg)
        tupAvg += [total]

        return tupAvg



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
        repo.authenticate('alice_bob', 'alice_bob')
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