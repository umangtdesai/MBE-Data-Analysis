# course-2019-spr-proj
Team Members: Vidya Akavoor, Lauren DiSalvo, Sreeja Keesara

Directory: ldisalvo_skeesara_vidyaap

## Project Justification
The below datasets can be combined to score the ideologies of voting districts within Massachusetts. The election data will help to identify which political party residents in that district typically vote for. The demographic census data can be used to determine predictors of voting patterns within each county and town. We plan to use the county shapes data to build our visualization of voting districts. We also plan to characterize each ballot question ideologically by comparing districts that voted in favor of the ballot question with those that voted for certain party candidates in the same year.  

## Datasets
### ballotQuestions
contains: data about each Massachusetts Ballot Question from 2000 to 2018

fields: question id, year, number, description, type, location

### ballotQuestionsResults
contains: data about voting results for each Massachusetts Ballot Question from 2000 to 2018

fields: question id, locality, ward, pct, yes votes, no votes, blanks, total votes cast

### stateHouseElections
contains: data about each Massachusetts General State House Election from 2000 to 2018

fields: election id, year, district, candidates (contains name, party, isWinner for each candidate

### stateHouseElectionsResults
contains: data about voting results for each Massachusetts General State House Election from 2000 to 2018

fields: election id, city/town, ward, pct, blanks, total votes cast, all others [fields for each candidate]

### stateSenateElections
contains: data about each Massachusetts General State Senate Election from 2000 to 2018

fields: election id, year, district, candidates (contains name, party, isWinner for each candidate

### stateSenateElectionsResults
contains: data about voting results for each Massachusetts General State Senate Election from 2000 to 2018

fields: election id, city/town, ward, pct, blanks, total votes cast, all others [fields for each candidate]

### countyShapes
contains: geoJSON data about each Massachusetts county (taken from Google Fusion Table and uploaded to datamechanics.io)

fields: _id, name, shape, geo_id

### demographicDataCounty
contains: demographic data by country from census.gov

fields: available at https://www.census.gov/quickfacts/fact/table/ma/PST045217

## Additional Python Libraries
You may need to import the following libraries to access our datasets: bs4, pandas, requests, csv, io

They can be installed by running the requirements.txt file

```
pip3 install requirements.txt
```