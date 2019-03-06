
# CS504 Data-Mechanics Project 1

## Authors
Haoxuan Jia(hxjia@bu.edu)
Jiahao Zhang(jiahaozh@bu.edu)

## Purpose

## Datasets
### Boston Airbnb Calendar
A csv file containing the price on a cetain day between year 2016 and 2017
http://datamechanics.io/data/hxjia_jiahaozh/Calendar.csv
### Boston Airbnb Listings
A csv file containing detailed information about a house
http://datamechanics.io/data/hxjia_jiahaozh/Listings.csv
### Boston Airbnb Reviews
A csv file containing reviews given by users to a specific house
http://data.insideairbnb.com/united-states/ma/boston/2019-01-17/data/reviews.csv.gz
### Boston Landmarks
A csv file containing landmarks in different neighborhoods of Boston
http://bostonopendata-boston.opendata.arcgis.com/datasets/7a7aca614ad740e99b060e0ee787a228_3.csv
### US Holidays
A csv file containing date and holidays from 1966 to 2011
http://datamechanics.io/data/hxjia_jiahaozh/US_Holidays.csv
## Data Transformation
### Price_and_Comments
Generated from Boston Airbnb Listings and Boston Airbnb Reviews
Boston Airbnb Listings: Project to get data in a form of (listing_id, price, review_score)
Boston Airbnb Reviews: Project to get data in a form of (listing_id, comments)
                       Aggregate to get a combination of comments for each listing_id
Combination: product + Project to get the comments and review score for each listing_id, in the form of (listing_id, price, comments, review_score)
### Price_on_Holidays
Generated from Boston Airbnb Calendar and US_Holidays
Boston Airbnb Calendar: Select the data with valid price
                        Project to get data in a form of (date, price)
                        Aggregate to get the mean price for each date
US_Holidays: Select holidays in the range of Boston Airbnb Calendar's dates
Combination: Product + Project to get the mean price for each date and whether the date is a holiday, in the form of (date, avg_price, holiday)
### Prices_Landmarks_Listings
## Tools
Pandas
dml
prov
protoql
