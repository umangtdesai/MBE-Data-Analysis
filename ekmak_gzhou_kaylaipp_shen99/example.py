import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import zillow 
import requests
import xmltodict
import csv

class example(dml.Algorithm):

    contributor = 'ekmak_gzhou_kaylaipp_shen99'
    reads = []
    writes = ['ekmak_gzhou_kaylaipp_shen99.zillow_property_data','ekmak_gzhou_kaylaipp_shen99.permit_data']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ekmak_gzhou_kaylaipp_shen99','ekmak_gzhou_kaylaipp_shen99')

        # url = 'http://cs-people.bu.edu/lapets/591/examples/lost.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("lost")
        # repo.createCollection("lost")
        # repo['alice_bob.lost'].insert_many(r)
        # repo['alice_bob.lost'].metadata({'complete':True})
        # print(repo['alice_bob.lost'].metadata())

        # url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # repo.dropCollection("found")
        # repo.createCollection("found")
        # repo['alice_bob.found'].insert_many(r)

        #Retrieve Zillow Property Data and add to mongo 
        url = 'http://datamechanics.io/data/zillow_property_data.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("zillow_property_data")
        repo.createCollection("zillow_property_data")
        for key,val in r.items(): 
            c = {'address':key, 'response':val}
            repo['ekmak_gzhou_kaylaipp_shen99.zillow_property_data'].insert_one(c)
        repo['ekmak_gzhou_kaylaipp_shen99.zillow_property_data'].metadata({'complete':True})
        print(repo['ekmak_gzhou_kaylaipp_shen99.zillow_property_data'].metadata())


        #Retrive permit database data and add to mongo
        url = 'http://datamechanics.io/data/ekmak_gzhou_kaylaipp_shen99/boston_permits.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['records']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("permit_data")
        repo.createCollection("permit_data")
        for permit in r: 
            permit_num = permit[0]
            worktype = permit[1]
            type_description = permit[2]
            description = permit[3]
            comments = permit[4]
            applicant = permit[5]
            declared_valuation = permit[6]
            total_fees = permit[7]
            issued_date = permit[8]
            expiration_date = permit[9]
            status = permit[10]
            owner = permit[11]
            occupancy_type = permit[12]
            sq_feet = permit[13]
            address = permit[14]
            city = permit[15]
            state = permit[16]
            zipcode = permit[17]
            location = permit[20]
            c = {'permit_num':permit_num, 'worktype':worktype,'type_description':type_description,
            'description':description, 'comments':comments, 'applicant':applicant, 
            'declared_valuation':declared_valuation, 'total_fees':total_fees, 'issued_date':issued_date,
            'expiration_date':expiration_date, 'status':status, 'owner':owner, 'occupancy_type':occupancy_type,
            'sq_feet':sq_feet, 'address':address, 'city':city, 'state':state, 'zipcode':zipcode, 'location':location}
            repo['ekmak_gzhou_kaylaipp_shen99.permit_data'].insert_one(c)

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
        repo.authenticate('alice_bob', 'alice_bob')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc


'''

Helper functions to retrieve data 

'''
#connect to zillow api
zillow_key = 'X1-ZWz1gx7ezhy3uz_9abc1'
api = zillow.ValuationApi()

#params: address,city,zipcode
#returns: dictionary of property data 
def get_property_results(address, city, state, zipcode): 
    #create empty dictionary to store results 
    house_details = {}
    full_address = address + ',' + city + ',' + state
    #first get zillow pid
    try: 
        data = api.GetSearchResults(zillow_key, full_address, zipcode)
    except zillow.error.ZillowError as e: 
        house_details[address] = {'posting': 'None'}
        return house_details
    zpid = data.zpid

    zillow_property_details = 'http://www.zillow.com/webservice/GetUpdatedPropertyDetails.htm?zws-id=' + zillow_key + '&zpid=' + zpid
    prop_details = requests.get(zillow_property_details)

    data = prop_details.content.decode('utf-8')
    xmltodict_data = xmltodict.parse(data)
    
    json_data = json.dumps(xmltodict_data)
    json_data = json.loads(json_data)

    api_code = json_data['UpdatedPropertyDetails:updatedPropertyDetails']['message']['code']

    if api_code == '0': 
        response = json_data['UpdatedPropertyDetails:updatedPropertyDetails']['response']
        street_ = response['address']['street']
        house_details[street_] = response

    else: 
        house_details[address] = {'posting': 'None'}
    return house_details



def retrieve_zillow_property_data(): 
    result = {}
    CSV_URL = 'http://datamechanics.io/data/Live_Street_Address_Management_SAM_Addresses.csv'
    with requests.get(CSV_URL, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        idx = 0
        for row in csv.reader(lines):
            print(idx)
            if idx % 5000 == 0: 
                print(idx)
            zipcode = row[20]
            street = row[5]
            if zipcode == '02127': 
                idx += 1
                data = get_property_results(street, 'Boston', 'MA', zipcode)
                result.update(data)
    print(result)
    #write out results to file
    with open('zillow_property_data.json', 'w') as outfile:
        print('outputing file')
        json.dump(result, outfile)



'''
# This is example code you might use for debugging this module.
# Please remove all top-level function calls before submitting.
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
example.execute()
## eof