import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import io
class PrivateSchool(dml.Algorithm):
    contributor = 'ruipang_zhou482'
    reads = []
    writes = ['ruipang_zhou482.PrivateSchool']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ruipang_zhou482', 'ruipang_zhou482')
        print ("abcdefg")
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.csv'
        response = urllib.request.urlopen(url)
        cr = csv.reader(io.StringIO(response.read().decode('utf-8')), delimiter = ',')
        ps =[]
        i =0
        dic={}
        for row in cr:
            if(i != 0):
                print (row[10])
                if row[10] in dic:
                    dic[row[10]]+=1
                else:
                    dic[row[10]] =1
        #             dic[row[7]][0]+=float(row[18])
        #             dic[row[7]][1]+=1
        #         else:
        #             p =[]
        #             p.append(float(row[18]))
        #             p.append(1)
        #             dic[row[7]]=p

            i = i + 1
        # print (dic['02108'])
        for i in dic:
            c = {}
             
            c['zipcode'] = i
            c['num_school']=dic[i]
            ps.append(c)
        print(ps)
        repo.dropCollection("PrivateSchool")
        repo.createCollection("PrivateSchool")
        
        
        repo['ruipang_zhou482.PrivateSchool'].insert_many(ps)
        repo['ruipang_zhou482.PrivateSchool'].metadata({'complete':True})
        # # print(repo['ruipang_zhou482.PropertyAssessment'].metadata())
        
        repo.logout()
        
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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

        this_script = doc.agent('alg:ruipang_zhou482#PrivateSchool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:Boston Property Values',
                              {'prov:label': 'Boston Private School Data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        get_propvalue = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_propvalue, this_script)
        doc.usage(get_propvalue, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        propvalue = doc.entity('dat:ruipang_zhou482#PrivateSchool', {prov.model.PROV_LABEL:'Boston PrivateSchool ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(propvalue, this_script)
        doc.wasGeneratedBy(propvalue, get_propvalue, endTime)
        doc.wasDerivedFrom(propvalue, resource, get_propvalue, get_propvalue, get_propvalue)

        repo.logout()

        return doc

PrivateSchool.execute()
doc = PrivateSchool.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))