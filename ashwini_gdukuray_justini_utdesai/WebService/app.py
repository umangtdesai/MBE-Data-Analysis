#!flask/bin/python
from flask import Flask, jsonify, abort, make_response, request, render_template
from pymongo import MongoClient
import pandas as pd

app = Flask(__name__, template_folder='template')

@app.route('/')
def index():
    data = ""
    return render_template('Home.html', data=data)

@app.route('/app/api/v0.1/industryTotals', methods=['GET'])
def getIndTotals():
    client = MongoClient()
    repo = client.repo
    repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
    indTotal = repo['ashwini_gdukuray_justini_utdesai.industryTotal']
    indTotalDF = pd.DataFrame(list(indTotal.find()))

    data = "Industry$$$Number of Businesses+++"
    for index, row in indTotalDF.iterrows():
        data += row['Industry'] + "$$$"
        data += str(row['Number of Businesses'])
        data += '+++'
        
    return render_template('Home.html', data = data)

@app.route('/app/api/v0.1/correlationCoefficients', methods=['GET'])
def getCorrCoeff():
    client = MongoClient()
    repo = client.repo
    repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
    corrCoeffs = repo['ashwini_gdukuray_justini_utdesai.correlations']
    corrCoeffsDF = pd.DataFrame(list(corrCoeffs.find()))

    data = "Industry 1$$$Industry 2$$$Correlation Coefficient+++"
    for index, row in corrCoeffsDF.iterrows():
        inds = row['Industries'].split(':')
        data += inds[0] + '$$$'
        data += inds[1] + '$$$'
        data += str(row['Correlation Coefficient'])
        data += '+++'
        
    return render_template('Home.html', data = data)

@app.route('/app/api/v0.1/masterList', methods=['GET'])
def getMasterList():
    client = MongoClient()
    repo = client.repo
    repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
    masterList = repo['ashwini_gdukuray_justini_utdesai.mergedList']
    masterListDF = pd.DataFrame(list(masterList.find()))

    data = "Business Name$$$Industry$$$Address$$$City$$$State$$$Zip$$$MBE Status+++"
    for index, row in masterListDF.iterrows():
        data += row['Business Name'] + "$$$"
        data += row['IndustryID'] + "$$$"
        data += row['Address'] + "$$$"
        data += row['City'] + "$$$"
        data += row['State'] + "$$$"
        data += row['Zip'] + "$$$"
        data += row['MBE Status']
        data += '+++'
        
    return render_template('Home.html', data = data)

@app.route('/app/api/v0.1/optimalLocations', methods=['GET'])
def getOptLocations():
    client = MongoClient()
    repo = client.repo
    repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
    optLocations = repo['ashwini_gdukuray_justini_utdesai.optimalLocations']
    optLocationsDF = pd.DataFrame(list(optLocations.find()))

    data = "Zip Code$$$Number of MBE Additions+++"
    for index, row in optLocationsDF.iterrows():
        data += row['Zip'] + '$$$'
        data += str(row['Number of MBE Additions'])
        data += '+++'
        
    return render_template('Home.html', data = data)

@app.route('/app/api/v0.1/topCompanies', methods=['GET'])
def getTopCompanies():
    client = MongoClient()
    repo = client.repo
    repo.authenticate('ashwini_gdukuray_justini_utdesai', 'ashwini_gdukuray_justini_utdesai')
    topCompanies = repo['ashwini_gdukuray_justini_utdesai.topCertCompanies']
    topCompaniesDF = pd.DataFrame(list(topCompanies.find()))

    data = "Business Name$$$2017 Revenue$$$Massachusetts Employees$$$"
    data += "Minority/Women Ownership Percentage$$$Address$$$City$$$"
    data += "State$$$Zip Code+++"
    for index, row in topCompaniesDF.iterrows():
        data += row['Business Name_x'] + '$$$'
        data += str(row['2017 Revenue']) + '$$$'
        data += str(row['Massachusetts Employees']) + '$$$'
        data += str(row['Minority/Women Ownership Percentage']) + '$$$'
        data += row['Address'] + '$$$'
        data += row['City'] + '$$$'
        data += row['State'] + '$$$'
        data += row['Zip']
        data += '+++'
        
    return render_template('Home.html', data = data)



if __name__ == '__main__':
    app.run(debug=True)
