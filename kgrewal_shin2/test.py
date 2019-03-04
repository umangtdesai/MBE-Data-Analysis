import requests
import json



url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/boston.geojson"
response = requests.get(url)

responseTxt = '[' + response.text + ']'
r = json.loads(responseTxt)

print(r)