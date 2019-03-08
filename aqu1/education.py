import json
import pandas as pd

reads = []  
writes = ['aqu1.education_data']

def education():
    # Dataset 3: Educational Attainment for People Ages 25+
    url = 'https://data.boston.gov/dataset/8202abf2-8434-4934-959b-94643c7dac18/resource/bb0f26f8-e472-483c-8f0c-e83048827673/download/educational-attainment-age-25.csv'
    education = pd.read_csv(url)

    # select for education attainment in 2000s
    education = education[education.Decade == 2000] 
    # projection: remove % from percent of population column
    education['Percent of Population'] = education['Percent of Population'].apply(lambda p: str(p)[:-1]) 
    education = pd.DataFrame(education)
    education = json.loads(education.to_json(orient = 'records')) 
    return education