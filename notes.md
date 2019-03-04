
# Some Notes
* To work this project, use the virtualenv in your cs504 directory.
* Launch mongo with: "mongod --auth --dbpath ./mongo-data" where the folder is in cs504.
* Run Mongo with username and password and load helper functions (described in README).

# TODO (& Useful Links)
* https://data.boston.gov/dataset/existing-bike-network
* http://geojson.org/
* https://data.boston.gov/dataset/city-of-boston-boundary2
* https://data.boston.gov/dataset/contours
* https://www.bluebikes.com/system-data
* https://data.boston.gov/dataset/park-boston-monthly-transactions-by-zone-2015 (ehhhhh)
* https://data.boston.gov/dataset/traffic-related-data
* Make sure to filter for universities w/ greater than 100 students
* Noticed a "TEMPORARY WINTER LOCATION" string in bluebikes data
* Don't forget to implement a "trial" flag for the algorithm

# More TODO
* provenance for mbta_stops script
* download bluebike stations (and do prov)
* download mbta alerts
	* one for just boston trains
	* one for all routes close to boston
	* don't forget provenance
	* I have notes for this one
* weekday time frequency for bluebike trips (algo and prov)
* bluebike stations within a mile (or half?) of universities
* where are bluebikers coming to school from
* bluebike correlation with temperature and precipitation data
* mbta boston train "delay" alert correlation to weather and time
* mbta "delay" alert correlation with traffic

# Final TODO
* Write up README for submission
* look at other project 1 requirements before submitting

# Ideas
* See how many bike lanes and hubways are near each university
* See average age of bikers near a university versus not near a university
* See distance biked and speed as compared to age of rider
* See where students live (people who bike to and from university of a certain age)
* See how far university students bike as compared to other bikers
* compare the speed of taking the train versus take a citibike to BU from elsewhere

# Useful DB Queries
* db.kgarber.university.find({"properties.NumStudent": {$gt: 200}}, {"properties.Name":1, "properties.NumStudent":1, "\_id":0}).sort({"properties.NumStudent":-1}).limit(15);
* db.kgarber.university.aggregate([{$match: {}}, {$group: {\_id: null, total: {$sum: "$properties.NumStudents"}}}])

# Used Datasets

Satisfies 5 datasets from at least 3 sources.

* BlueBike
	* stations
	* trips
* data.boston.gov
	* universities
* MBTA
	* stops
	* trips
	* routes
	* alerts
* NOAA
	* boston daily weather 2018

# Output Datasets

Satisfies at least 3 non-trivial new datasets from transformations and combinations.

Main:
* bluebike usage correlation with weather
* origins of university bluebike riders
* mbta alerts correlation to weather

Other:
* bluebike number rides per date
* bluebike stations near universities

# Other (old)

***What I'm Doing***

Investigating how people in Boston commute, namely insights into public transit in Boston, what we can learn about the users, and the shortcomings of the system that we can looks at.

***Reasoning***

TODO...

***Datasets***

Here are the datasets I'd like to use, as well as the features they have.

* Boston BlueBike
    * TODO...
* Boston.gov Universities information
    * TODO...
* Census for Boston Neighborhoods
    * TODO...
* MBTA Performance
    * TODO...
* MBTA All Stations (locations)
	* TODO...
* OpenStreetMap
    * TODO...
* Boston Historic Weather
    * TODO...
* Boston "Existing Bike Network"

***Transformations***

TODO...
