# From https://www.cityofboston.gov/images_documents/PETDESIG_JAN2014_tcm3-40308.pdf
import pandas as pd

file_name = 'landmarks.csv'

df = pd.read_csv(file_name)


df = df.drop('Petition Number', axis=1)

print(df.head(5))


out = df.to_json(orient='records')[1:-1].replace('},{', '} {')

with open('boston_landmarks.json', 'w') as f:
    f.write(out)
