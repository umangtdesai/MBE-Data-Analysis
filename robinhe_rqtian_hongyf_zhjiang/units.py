import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class units(dml.Algorithm):
    contributor = 'robinhe_rqtian_hongyf_zhjiang'
    reads = []
    writes = ['units']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')

        annualUnits = repo['robinhe_rqtian_hongyf_zhjiang.City of Revere Units Added']

        annualUnitsDF = pd.DataFrame(list(annualUnits.find()))

        annualUnitsDF = annualUnitsDF[['Year','Units']]
        # build sum of each industry
        data = {}
        for years, units in annualUnitsDF.iterrows():
            year = row['Year']
            if (year in data):
                data[year] += units
            else:
                data[industry] = units

        listOfUnits = list(data)
        listOfTuples = []
        for ind in listOfUnits:
            listOfTuples.append((year, data[year]))

        AnnualUnitsIncreaseDF = pd.DataFrame(listOfTuples, columns = ['Year', 'Number of Units Added'])

        AnnualUnitsIncreaseDF = AnnualUnitsIncreaseDF.sort_values(by=['Number of Units Added'], ascending=False)

        #print(AnnualUnitsIncreaseDF)

        #records = json.loads(AnnualUnitsIncreaseDF.T.to_json()).values()

        
        repo.dropCollection("units")
        repo.createCollection("units")
        repo['robinhe_rqtian_hongyf_zhjiang.units'].insert_many(outputs)
        repo['robinhe_rqtian_hongyf_zhjiang.units'].metadata({'complete':True})
        print(repo['robinhe_rqtian_hongyf_zhjiang.units'].metadata())

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
        
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
        repo.authenticate('robinhe_rqtian_hongyf_zhjiang', 'robinhe_rqtian_hongyf_zhjiang')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.

        # Data resource
        doc.add_namespace('dsrc', 'http://datamechanics.io/data/robinhe_rqtian_hongyf_zhjiang/')

        this_script = doc.agent('alg:robinhe_rqtian_hongyf_zhjiang#units_aggregation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dsrc:unit_original',
                              {'prov:label': 'original unit data', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        aggregate_units = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(aggregate_units, this_script)
        doc.usage(aggregate_units, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})

        crash = doc.entity('dat:robinhe_rqtian_hongyf_zhjiang#units',
                          {prov.model.PROV_LABEL: 'crash data from 01 to 19', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(units, this_script)
        doc.wasGeneratedBy(units, aggregate_units, endTime)
        doc.wasDerivedFrom(units, resource, aggregate_units, aggregate_units, aggregate_units)

        repo.logout()

        return doc

units.execute();
doc = units.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))