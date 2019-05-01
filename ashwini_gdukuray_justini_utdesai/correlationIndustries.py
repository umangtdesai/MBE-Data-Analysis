import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
import statistics as st
import matplotlib.pyplot as plt


class correlationIndustries(dml.Algorithm):
    contributor = 'ashwini_gdukuray_justini_utdesai'
    reads = ['ashwini_gdukuray_justini_utdesai.mergedList']
    writes = ['ashwini_gdukuray_justini_utdesai.correlations']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')

        mergedList = repo['ashwini_gdukuray_justini_utdesai.mergedList']

        if (trial):
            mergedListDF = pd.DataFrame(list(mergedList.find()))[:100]
        else:
            mergedListDF = pd.DataFrame(list(mergedList.find()))

        # Break dataframe into separate dataframes by zip code
        zipCodeDF = mergedListDF.groupby('Zip')
        zipGroups = [zipCodeDF.get_group(x) for x in zipCodeDF.groups]

        # Create totals for each industry per zip code
        indTotalsPerZip = {}

        for grp in zipGroups:
            currentZip = grp.iloc[0]['Zip']
            indTotalsPerZip[currentZip] = {}

            for index, row in grp.iterrows():
                ind = row['IndustryID']

                if (ind in indTotalsPerZip[currentZip]):
                    indTotalsPerZip[currentZip][ind] += 1
                else:
                    indTotalsPerZip[currentZip][ind] = 1

        industries = list(set(mergedListDF['IndustryID']))

        # Build 2 vectors per industry pairing to reflect the number of times each industry occurs in a particular zip code
        # The vector length will be the number of zip codes as each value represents the number of times that industry occurs there
        # Food:Architecture -> key
        # [(1,2,3), (0,4,6)] -> value
        vectorDict = {}
        for ind in industries:
            for grp in zipGroups:
                zip = grp.iloc[0]['Zip']

                # iterate through industries for this zip code
                for i in indTotalsPerZip[zip]:
                    # make sure not computing a correlation coefficient with itself
                    if (ind != i):
                        key = ind + ':' + i
                        if (key in vectorDict):
                            vectorDict[key][0] += (indTotalsPerZip[zip].get(ind, 0),)
                            vectorDict[key][1] += (indTotalsPerZip[zip][i],)
                        else:
                            vectorDict[key] = [(indTotalsPerZip[zip].get(ind, 0),)]
                            vectorDict[key].append((indTotalsPerZip[zip][i],))

                # add 0s for industries that did not exist in current zip
                missing = list(set(industries) - set(indTotalsPerZip[zip]))
                for j in missing:
                    # make sure not computing a correlation coefficient with itself
                    if (ind != j):
                        key = ind + ':' + j
                        if (key in vectorDict):
                            vectorDict[key][0] += (indTotalsPerZip[zip].get(ind, 0),)
                            vectorDict[key][1] += (0,)
                        else:
                            vectorDict[key] = [(indTotalsPerZip[zip].get(ind, 0),)]
                            vectorDict[key].append((0,))

        # Remove duplicates from vectorDict (i.e. if we have Food:Services, Services:Food also exists in dictionary with identical values
        keys = list(vectorDict)
        safeKeys = []
        for k in keys:
            kSplit = k.split(':')
            reverse = kSplit[1] + ':' + kSplit[0]
            # If the reverse key exists, pop it out of the dictionary
            if (reverse in vectorDict and reverse not in safeKeys):
                vectorDict.pop(reverse, None)
                # Keep this duplicate
                safeKeys.append(k)

        # compute correlation for each pair of vectors
        correlationDict = {'Industries': [], 'Correlation Coefficient': []}
        for pair in vectorDict:
            vecx = vectorDict[pair][0]
            vecy = vectorDict[pair][1]
            vecLen = len(vecx)

            # Compute covariance

            # Compute mean values
            meanx = st.mean(vecx)
            meany = st.mean(vecy)

            preX = []
            preY = []
            for i in range(vecLen):
                preX.append(vecx[i] - meanx)
                preY.append(vecy[i] - meany)

            dotProduct = 0
            for i in range(vecLen):
                dotProduct += (preX[i] * preY[i])

            covariance = (1/vecLen) * dotProduct

            # Compute standard deviations
            stdX = st.stdev(vecx)
            stdY = st.stdev(vecy)

            correlation = covariance / (stdX * stdY)

            correlationDict['Industries'].append(pair)
            correlationDict['Correlation Coefficient'].append(correlation)

        correlationDF = pd.DataFrame(correlationDict)
        correlationDF = correlationDF.sort_values('Correlation Coefficient', ascending=False)
        correlationDF = correlationDF.reset_index(drop=True)

        # Code to plot
        # These industries occur so infrequently that the correlation coefficients may not be super accurate
        excludedInds = ['Music', 'Answering Services', 'Consumer Services', 'Education', 'Counseling', 'Fencing', 'Floral', 'Research', 'Environment', 'Automobile']

        count = 0
        xVals = []
        yVals = []
        for index, row in correlationDF.iterrows():
            flag = True
            if count == 10:
                break

            pairOfInds = row['Industries']

            # make sure the pair doesn't included an excluded industry, set the flag if this happens
            for ban in excludedInds:
                if ban in pairOfInds:
                    flag = False
                    break

            if flag:
                xVals.append(pairOfInds)
                yVals.append(row['Correlation Coefficient'])
                count += 1
        '''
        inds = pd.Series(data=yVals, index=xVals)
        plt.figure(figsize=(12, 8))
        plt.title("Most Correlated Industries")
        #plt.gcf().subplots_adjust(bottom=0.30)
        inds.head(n=15).plot.bar()
        plt.savefig("Plot.png")
        plt.show()
        '''

        #print(correlationDF)

        #records = json.loads(industryDF.T.to_json()).values()

        repo.dropCollection("correlations")
        repo.createCollection("correlations")
        repo['ashwini_gdukuray_justini_utdesai.correlations'].insert_many(correlationDF.to_dict('records'))
        repo['ashwini_gdukuray_justini_utdesai.correlations'].metadata({'complete': True})
        print(repo['ashwini_gdukuray_justini_utdesai.correlations'].metadata())

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
        repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'http://datamechanics.io/?prefix=ashwini_gdukuray_justini_utdesai/')

        this_script = doc.agent('alg:ashwini_gdukuray_justini_utdesai#correlationIndustries()',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        mergedList = doc.entity('dat:ashwini_gdukuray_justini_utdesai#mergedList',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        correlations = doc.entity('dat:ashwini_gdukuray_justini_utdesai#correlations',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataSet',
                               'ont:Extension': 'json'})
        act = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(act, this_script)
        doc.usage(act, mergedList, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval',
                   'ont:Query': '?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                   }
                  )

        doc.wasAttributedTo(correlations, this_script)
        doc.wasGeneratedBy(correlations, act, endTime)
        doc.wasDerivedFrom(correlations, mergedList, act, act, act)

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
