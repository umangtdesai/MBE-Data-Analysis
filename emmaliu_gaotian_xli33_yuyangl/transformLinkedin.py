import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class transformLinkedin(dml.Algorithm):
    contributor = 'emmaliu_yuyangl'
    reads = ['emmaliu_gaotian_xli33_yuyangl.linkedin']
    writes = ['emmaliu_gaotian_xli33_yuyangl.userLocation']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emmaliu_gaotian_xli33_yuyangl', 'emmaliu_gaotian_xli33_yuyangl')
        
        url = ''
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("linkedin")
        repo.createCollection("linkedin")
        repo['emmaliu_gaotian_xli33_yuyangl.linkedin'].insert_many(r)
        repo['emmaliu_gaotian_xli33_yuyangl.linkedin'].metadata({'complete': True})
        print(repo['emmaliu_gaotian_xli33_yuyangl.linkedin'].metadata())

        # Get Tweets data
        linkedinData = repo.emmaliu_gaotian_xli33_yuyangl.linkedin.find()
        jobs = []
        num_change=0
        # Filter for user's location, project key value pairs.
        for data in linkedinData:
            if data['query'] == "amman":
                name = data['name']
                location = data['query']
                job = data['job']
                currentJob = data['currentJob']
                if data['currentJob'] != null:
                    jobchange = True

                names[name] = {'jobchange':jobchange}
        
        for name in names:
            if data['jobchange']:
                names[name] = {'job':currentJob}
            else:
                names[name] = {'job':job}



        #with open("userLocation .json", 'w') as outfile:
         #   json.dump(dataStored, outfile, indent=4)

        # store results into database
        repo.dropCollection("userLocation")
        repo.createCollection("userLocation")

        for i in dataStored:
            # print(i)
            repo['emmaliu_gaotian_xli33_yuyangl.userLocation'].insert(i)
        repo['emmaliu_gaotian_xli33_yuyangl.userLocation'].metadata({'complete': True})
        print(repo['emmaliu_gaotian_xli33_yuyangl.userLocation'].metadata())

        repo.logout()

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
        repo.authenticate('emmaliu_gaotian_xli33_yuyangl', 'emmaliu_gaotian_xli33_yuyangl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/emmaliu_gaotian_xli33_yuyangl')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/emmaliu_gaotian_xli33_yuyangl')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', '')
        this_script = doc.agent('alg:emmaliu_gaotian_xli33_yuyangl#transformLinkedin',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#linkedin',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        transform_linkedin = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(transform_linkedin, this_script)
        doc.usage(transform_linkedin, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:calculation',
                   'ont:Query': ''
                   }
                  )
        userLocation = doc.entity('dat:emmaliu_gaotian_xli33_yuyangl#get_linkedin',
                                  {prov.model.PROV_LABEL: 'linkedin from Amman', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(userLocation, this_script)
        doc.wasGeneratedBy(userLocation, transform_linkedin, endTime)
        doc.wasDerivedFrom(userLocation, resource, transform_linkedin, transform_linkedin, transform_tweets)

        repo.logout()

        return doc

transformLinkedin.execute()
# doc = getTweets.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
