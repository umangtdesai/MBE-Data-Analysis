Inspiration: 

Bluebikes is a public bike share system in Boston, Brookline, Cambridge and Somerville. I ride their bike to BU everyday and it's really convenient. However, sometimes I find it hard to find a bike or a dock to park my bike because the bike stations are not located very reasonably. For example, the Bluebike station at BU campus has only 10 docks, and I have to look for another bike station nearby sometimes because that one full. That actually made me late for CS504 class for twice this semester! So I am thinking, if Bluebike can learn more about their bike-using situation and set their bike stations more properly, it will be very nice for people who use their bike service like me. Thus in this project, I will try to find out which places are good choices to place a Bluebike station and how many docks each station should have.


Datasets:

There are five datasets at this time, each has a retrieve algorithm in separated python files:

Subway stop locations: http://datamechanics.io/data/yufeng72/Subway_Stops.json

us stop locations: http://datamechanics.io/data/yufeng72/Bus_Stops.csv

College and university locations: http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.csv

Bluebike station locations: https://s3.amazonaws.com/hubway-data/Hubway_Stations_as_of_July_2017.csv

Bluebike trip data 2018.9: http://datamechanics.io/data/yufeng72/Bluebikes_Tripdata_201809.csv


Transformations:

Implemented 3 transformations for now, all three using selection, projection and combination:

Transformation 1: find bus stops, colleges and universities with valid latitude and longitude in Boston to find possible places for placing bike stations.

Transformation 2: calculate the distance between every Bluebike station and every college & university, then count the number of the Bluebike stations near these colleges and universities.

Transformation 3: for each college and university, find out how many Bluebike trips take them as destination (in a month) to see if it is popular.
