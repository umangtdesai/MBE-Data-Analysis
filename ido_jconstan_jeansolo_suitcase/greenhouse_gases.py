import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class greenhouse_gases(dml.Algorithm):
    contributor = 'ido_jconstan_jeansolo_suitcase'
    reads = ['ido_jconstan_jeansolo_suitcase.greenhouse_emissions']
    writes = ['ido_jconstan_jeansolo_suitcase.greenhouse_emissions']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ido_jconstan_jeansolo_suitcase', 'ido_jconstan_jeansolo_suitcase')
        

        gg = repo.ido_jconstan_jeansolo_suitcase.greenhouse_emissions.find()
        
        gglist = []
        nvcount = 0
        for item in gg: 
            new_dict = {}
            new_dict['VCount'] = vcount
            if (feature['properties']['Source'] == 'Vehicle Fuel'):
                new_dict['VCount'] = new_dict['VCount']+1
            else:
                nvcount+=1
            new_dict[nvCount] = nvcount
            new_dict[vPercentage] = new_dict['VCount'] / (nvCount+new_dict['VCount'])
            gglist.append(new_dict)
        repo['ido_jconstan_jeansolo_suitcase.greenhouse_emissions'].insert_many(gglist)
        for t in gg:
            print(t)

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        doc.add_namespace('dbg', 'https://data.boston.gov/dataset/greenhouse-gas-emissions/resource/')
        this_script = doc.agent('alg:ido_jconstan_jeansolo_suitcase#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_gasEmissions = doc.entity('dbg:bd8dd4bb-867e-4ca2-b6c7-6c3bd9e6c290', {'prov:label':'Gas Emissions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_gas_emissions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_gas_emissions, this_script)
        doc.usage(get_gas_emissions, resource_gasEmissions, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
                  
        gas_emissions = doc.entity('dat:ido_jconstan_jeansolo_suitcase#gas_emissions', {prov.model.PROV_LABEL:'Greenhouse Gas Emissions', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(gas_emissions, this_script)
        doc.wasGeneratedBy(gas_emissions, get_gas_emissions, endTime)
        doc.wasDerivedFrom(gas_emissions, resource_gasEmissions, get_gas_emissions, get_gas_emissions, get_gas_emissions)
        
        repo.logout()
                  
        return doc
