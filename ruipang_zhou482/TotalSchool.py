import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io

class TotalSchool(dml.Algorithm):
    
    contributor = 'ruipang_zhou482'
    reads = ['ruipang_zhou482.publicSchool', 'ruipang_zhou482.privateSchool']
    writes = ['ruipang_zhou482.TotalSchool']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')

        # This is the helper function to access the database and get the data from the tables
        def getCollection(db):
            temp = []
            for i in repo['ruipang_zhou482.' + db].find():
                temp.append(i)
            return temp


        # Get the data from the publicSchool table
        public_school = getCollection('PublicSchool')
        # Get the data from the privateSchool table
        private_school = getCollection('PrivateSchool')

    
        print (private_school)
        print("ssss")
        total = private_school+public_school
        print (total)

        s=[]
        keys = {r['zipcode'] for r in total}
        for k in keys:
            dic={}
            sum=0
            for r in total:
                if r['zipcode'] == k:
                    sum+=r['num_school']
            dic['zipcode'] = k
            dic['total_school'] = sum
            s.append(dic)




        # Create the table called allSchool and save the data in the database
        #repo.authenticate('debhe_wangdayu', 'debhe_wangdayu')
        repo.dropCollection('ruipang_zhou482.TotalSchool')
        repo.createCollection('ruipang_zhou482.TotalSchool')
        repo['ruipang_zhou482.allSchool'].insert_many(s)
        repo['ruipang_zhou482.allSchool'].metadata({'complete':True})
        # print(repo['debhe_wangdayu.allSchool'].metadata())


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ruipang_zhou482#TotalSchool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Boston Property Values',
                              {'prov:label': 'Boston Total School', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_propvalue = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_propvalue, this_script)
        doc.usage(get_propvalue, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        propvalue = doc.entity('dat:ruipang_zhou482#TotalSchool', {prov.model.PROV_LABEL:'Boston TotalSchool ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(propvalue, this_script)
        doc.wasGeneratedBy(propvalue, get_propvalue, endTime)
        doc.wasDerivedFrom(propvalue, resource, get_propvalue, get_propvalue, get_propvalue)

        repo.logout()

        return doc


TotalSchool.execute()



