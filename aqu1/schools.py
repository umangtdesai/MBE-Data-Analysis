import json
import pandas as pd


reads = []
writes = ['aqu1.schools_data']

def schools():
    # Dataset 1: Colleges in Boston
    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.csv'
    college = pd.read_csv(url)

    # Dataset 2: Public Schools in Boston
    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.csv'
    school = pd.read_csv(url)

    # Merge latitude and longitudes of all colleges and public schools in Boston
    colleges = pd.concat([college.Latitude, college.Longitude], axis = 1) # select columns
    schools = pd.concat([school.Y, school.X], axis = 1) # select columns 
    schools.columns = ['Latitude', 'Longitude']
    all_schools = colleges.append(schools) # aggregate data 
    all_schools = pd.DataFrame(all_schools)
    all_schools = json.loads(all_schools.to_json(orient = 'records'))
    return all_schools