"""
CS504 : constants
Team : Vidya Akavoor, Lauren DiSalvo, Sreeja Keesara
Description :

Notes : 

February 28, 2019
"""

# mongodb collection names
TEAM_NAME = "ldisalvo_skeesara_vidyaap"

BALLOT_QUESTIONS = "ballotQuestions"
BALLOT_QUESTIONS_RESULTS = "ballotQuestionsResults"
BALLOT_QUESTIONS_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=BALLOT_QUESTIONS)
BALLOT_QUESTIONS_RESULTS_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=BALLOT_QUESTIONS_RESULTS)

STATE_HOUSE_ELECTIONS = "stateHouseElections"
STATE_HOUSE_ELECTIONS_RESULTS = "stateHouseElectionsResults"
STATE_HOUSE_ELECTIONS_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=STATE_HOUSE_ELECTIONS)
STATE_HOUSE_ELECTIONS_RESULTS_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=STATE_HOUSE_ELECTIONS_RESULTS)

STATE_SENATE_ELECTIONS = "stateSenateElections"
STATE_SENATE_ELECTIONS_RESULTS = "stateSenateElectionsResults"
STATE_SENATE_ELECTIONS_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=STATE_SENATE_ELECTIONS)
STATE_SENATE_ELECTIONS_RESULTS_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=STATE_SENATE_ELECTIONS_RESULTS)

DEMOGRAPHIC_DATA_COUNTY = "demographicDataCounty"
DEMOGRAPHIC_DATA_COUNTY_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=DEMOGRAPHIC_DATA_COUNTY)

# electionstats URLs
BALLOT_QUESTION_2000_2018_URL = "http://electionstats.state.ma.us/ballot_questions/search/year_from:2000/year_to:2018"
STATE_HOUSE_GENERAL_2000_2018_URL = "http://electionstats.state.ma.us/elections/search/year_from:2000/year_to:2018/office_id:8/stage:General"
STATE_SENATE_GENERAL_2000_2018_URL = "http://electionstats.state.ma.us/elections/search/year_from:2000/year_to:2018/office_id:9/stage:General"

BALLOT_QUESTION_DOWNLOAD_RESULTS_URL = "http://electionstats.state.ma.us/ballot_questions/download/{id}/precincts_include:1/"
ELECTION_DOWNLOAD_RESULTS_URL = "http://electionstats.state.ma.us/elections/download/{id}/precincts_include:1/"


# geographic data
MA_COUNTY_LIST = ["Barnstable County", "Berkshire County", "Bristol County", "Dukes County", "Essex County",
                  "Franklin County", "Hampden County", "Hampshire County","Middlesex County", "Nantucket County",
                  "Norfolk County", "Plymouth County", "Suffolk County", "Worcester County"]


# census.gov URLs
COUNTY_URL = "https://www.census.gov/quickfacts/fact/csv//PST045218"

#county shape data
FUSION_TABLE_URL = "http://datamechanics.io/data/massachusetts_counties.csv"
COUNTY_SHAPE = "countyShape"
COUNTY_SHAPE_NAME = "ldisalvo_skeesara_vidyaap.{name}".format(name=COUNTY_SHAPE)

