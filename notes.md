
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

# Ideas
* See how many bike lanes and hubways are near each university
* See average age of bikers near a university versus not near a university
* See distance biked and speed as compared to age of rider
* See where students live (people who bike to and from university of a certain age)
* See how far university students bike as compared to other bikers

# Useful DB Queries
* db.kgarber.university.find({"properties.NumStudent": {$gt: 200}}, {"properties.Name":1, "properties.NumStudent":1, "\_id":0}).sort({"properties.NumStudent":-1}).limit(15);
* db.kgarber.university.aggregate([{$match: {}}, {$group: {\_id: null, total: {$sum: "$properties.NumStudents"}}}])

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
