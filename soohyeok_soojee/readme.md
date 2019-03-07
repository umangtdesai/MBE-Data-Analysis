# CS504 Project#1: Proper Boston Tour

## Contributors:
- Soo Hyeok Lee
- Soojee Lee

### Project Goal:
Our goal is to determine best travel experiences for incoming tourists within Greater Boston Area. Having such an immense area, people may not have their best experiences in their limited time of travel and we wanted to suggest specific areas based on various datasets for the best possible experience.

### Data sources Used:
- Analyze Boston (data.boston.gov)
- Boston Maps Open Data (bostonopendata-boston.opendata.arcgis.com)
- Massachusetts Department of Transportation (geo-massdot.opendata.arcgis.com)

### Datasets Used:
1. Boston Neighborhoods (get_neighborhoods.py)  
https://data.boston.gov/dataset/boston-neighborhoods
2. Crime rate (get_crimeDta.py)  
https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system
3. Boston Landmarks Commission (BLC) Historic Districts (get_landmarks.py)   
http://bostonopendata-boston.opendata.arcgis.com/datasets/547a3ccb7ab443ceaaba62eef6694e74_4
4. MBTA Bus Stops (get_busStops.py)  
https://geo-massdot.opendata.arcgis.com/datasets/2c00111621954fa08ff44283364bba70_0
5. MBTA Station stops (get_trainStations.py)  
https://geo-massdot.opendata.arcgis.com/datasets/train-stations?geometry=-73.51%2C41.878%2C-69.555%2C42.59

### Currently Overlooked Transformations:
We currently put together a few datasets and transformed them to see which neighborhoods within the Greater Boston Area  
- has greater number of landmarks to see
- has better system of public transportation
- has low crime rates.

### Problems to Solve



### Execution Script for Provenance.html
To execute all the algorithms for the project in an order that respects their explicitly specified data flow dependencies, run the following from the root directory:
```
python execute.py soohyeok_soojee
```
