
# Dyanmic Chicago Transit Fares
Nathaniel Smith | BU: smithnj | github: njsmithh </br>
CS504 - Data Mechanics Project 1

## Proposal
The city of Chicago is the third largest city in the United States. Like other urban areas, it has a relatively robust transit network connecting the city and the surrounding suburbs. The Chicago Transit Authority ('CTA') is tasked with management of the Chicago bus and the elevated-rail ('L') networks. It opts for a static fare, independent of distance traveled within the network and other possible variables unlike a city such as London, which has designated zones for determining fare. The Metra commuter rail, which serves the greater Northwestern Illinois area, does use a distance-based fare. But what if the CTA took an approach similar to Metra rail, but also taking into account "surge" style pricing (with congestion and ridership data) and while maintaining a more stable price for transit in lower-income areas?

## Data Sets

| Portal   | Dataset                                                                                                                             | Notes 
|----------|-------------------------------------------------------------------------------------------------------------------------------------| ----
| Chicago Data Portal      | ['L' Station Ridership Stats](https://data.cityofchicago.org/Transportation/CTA-Ridership-L-Station-Entries-Daily-Totals/5neh-572f) | Received as .json
| Chicago Data Portal    | [Chicago Neighborhoods](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Neighborhoods/bbvz-uum9) | Received as .geojson                                                                                                       |
| Chicago Data Portal    | [Census Socioeconomic Indicatiors](https://data.cityofchicago.org/Health-Human-Services/Census-Data-Selected-socioeconomic-indicators-in-C/kn9c-c2s2)    |Received as .json
| Data.gov | [Chicago Congestion](https://catalog.data.gov/dataset/chicago-traffic-tracker-historical-congestion-estimates-by-region-a0e83)    | Received as .json                                             |                                                                                                         |
| datamechanics.io      | ['L' Station Locations](http://datamechanics.io/?prefix=smithnj/) | Retrieved from CTA in .klm, manually converted to .geojson
---
#### Library Dependencies
* pandas, geopandas
* JSON