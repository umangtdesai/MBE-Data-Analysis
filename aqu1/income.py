import json
import pandas as pd 

reads = []
writes = ['aqu1.income_data']

def income():
    # Dataset 4: Vulnerable Groups in Boston 
    url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/34f2c48b670d4b43a617b1540f20efe3_0.csv'
    vulnerable = pd.read_csv(url)

    vulnerable = vulnerable.groupby('Name').sum() # aggregate data for 23 neighborhoods of Boston
    vulnerable['prop_low_income'] = vulnerable.Low_to_No / vulnerable.POP100_RE # projection: new column for proportion of people who have low income
    vulnerable = pd.DataFrame(vulnerable)
    vulnerable = json.loads(vulnerable.to_json(orient = 'records')) 
    return vulnerable