# Project 1
For this problem, we aim to try to see what affects individual's health in Boston. More specifically, 
we intend to compare across different districts in Boston health survey data provided by the CDC against
various characteristics of these district, some of which include demographics and access to open spaces.
### Data Portals and Datsets Used
1. [Analyze Boston](https://data.boston.gov/dataset/)  
    - [Age demographics by neighborhood](https://data.boston.gov/dataset/8202abf2-8434-4934-959b-94643c7dac18/resource/c53f0204-3b39-4a33-8068-64168dbe9847/download/age.csv)
    - [Race demographics by neighborhood](https://data.boston.gov/dataset/8202abf2-8434-4934-959b-94643c7dac18/resource/20f64c02-6023-4280-8131-e8c0cedcae9b/download/race-and-or-ethnicity.csv)
2. [Boston Maps Open Data](http://bostonopendata-boston.opendata.arcgis.com/)
    - [Open spaces geospatial data in Boston](http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson)
    - [Boston neighborhoods geospatial data](http://bostonopendata-boston.opendata.arcgis.com/datasets/3525b0ee6e6b427f9aab5d0a1d0a1a28_0.geojson)
3. [Center for Disease Control and Prevention](https://chronicdata.cdc.gov/)
    - [Health survey data in Boston](https://chronicdata.cdc.gov/resource/csmm-fdhi.json?cityname=Boston)
    
### Dependencies
Besides the dependencies needed to run ``execute.py`` as stated in the note 
we need to install the **Shapely** library which is used to process the 
geospatial data. To install on MacOS/Linux, run
```
pip install Shapely
```
On Windows you need to download and install the wheel file which can be found here:  
[Shapely.whl](http://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely)

### Algorithms
##### getData.py
This algorithm goes to each of the three data portals listed above
and retrieves the five datasets. The open space and neighborhood datasets are geojson files, the cdc health 
survey data is a json file, and the race demographics and age demographics data comes as a csv file. The algorithm
does a little extra work with the csv file, turning them into json files so that they can be properly inserted into
MongoDB. This algorithm doesn't modify any of the data, and each dataset is stored into its own mongo database with the 
prefix "Raw_".

##### combineDemographics.py
This is the first transformation. The goal of this algorithm is to take the raw data collected on race demographics
and age demographics and combine them into one dataset. To do this first for each dataset I did a select on the data 
choosing data collected most recently from the year 2010. Then, since each data value in the dataset was of the form (id, demographic type, neighborhood, ...)
I combined each demographic type(i.e Black, White, Asian for race and 0-9, 10-19, etc. for age) with the same neighbor hood under one row whose id was neighborhood
and columns was each type. Then with the two changed race and age datasets I merely did a join of the two sets by neighborhood(now the id) and stored this
joined data set in mongo as Age_Race 
##### percentOpenSpace.py
This is the second transformation. The goal of this algorithm was to take each neighborhood in Boston and compute the 
percentage of the area of the neighborhood that is covered by different types of open spaces suchs as "Parks, Playgrounds & Athletic Fields"
or "Urban Wilds & Natural Areas". However, there was some discrepency between the district that each open space was listed under 
and the neighborhoods denoted in the Raw_Neighborhoods dataset. To fix this, because the districts listed in the Raw_Open_Spaces were larger 
geospatial entities than the neighborhoods (at least one neighborhood fell in one of the districts) I decided that we would compute the total area of
the district by summing the total area of the neighborhood or neighborhoods that made up each district. Thus, I made a key map
that mapped districts to their respective neighborhoods. There wer some districts that did not need a mapping as its name and area 
wer identical to the name and area of the neighborhood listed in Raw_Neighborhoods. After this set up we then
merely aggregated each open space listed by district name, and then by open space type. Then we summed up the total area covered by 
each open space type in each district, and then divided these figures by the total are of the district which was computed with
our Raw_Neighborhood dataset along with our key map to then get a percentage of each district that is covered by each neighborhood.
Ideally, we would have hoped to not have mapped neighborhoods to district, but rather use the geojson data to see if the open space
was contained in one of our neighborhoods. Moving forward we would rather have this as our demographics data is in terms 
of these neighborhoods and not districts. This dataset is stored in mongo as Percent_OS.

##### combineNeighborhoodHealth.py
This is the third and last transformation. The goal of this algorithm is to take the cdc health survey data, where each row is of the form
(health question, answer value, coordinates, ...), and match each question's coordinate value to a neighborhood. This way we can get an understanding
of the neighborhood's health through the survey data. To match coordinates to a neighborhood, we had to use the
shapely library as outlined in the dependency section. This library allows us to construct the polygon or set of 
polygons that are represented by geojson coordinates for each neighborhood, as well as a point for each
of the coordinates. Then using the shapely method contains() in the Polygon class, we iterated through each
neighborhood to find if the point is within the polygon and thus the neighborhood. Then, we projected the health data 
to only the few values of interest (i.e question and answer value) that we wanted as well adding the neighborhood
that this survey took place in. We then store this data in mongo as Neighborhood_Health.

### Justification
These three transformations were motivated in trying to find the how open spaces effect health in Boston.
More specifically, in the future we look to regress the relative health scores collected by the CDC on amount of access people in these neighborhoods have to open spaces. That is why we first 
aggregated age and race demographics for each neighborhood. The reason for collecting demographic data as another explanatory variable
for health is that for one, age places a very important factor in health. Clearly, you would expect neighborhoods with older
residents to have poorer health than those that are younger. Similarly, more diverse communities have been shown to have poorer health
as typically these are lower income neighborhoods and limitations such as inability to speak English affect health as it is harder to 
request for health services. This regression would not be very accurate if we didn't include demographic data of each neighborhood as then we would suffer from omitted variable bias. With
that being said, ideally we would plan to include more important explanatory variables that could effect so that we could most accurately
capture the effect of open space. With the given transformations, much of the setup for this regression is done as we have catagorized the 
CDC health survey data under each neighborhood as well as collected the demographics of each neighborhood as well as the percentage of
open space in each district. All that is left to do is to quantify these health surveys into a relative "health score", or just
do a regression on a few of the important health questions. Also we would need to remedy the classification issue between neighborhoods and 
the districts listed for open spaces, but we could do this with the neighborhood to district mapping that we produced or by using 
Shapely to ignore district and find exactly in which neighborhood the open spaces lie in.

### Run
To run simply enter:
```
python execute.py tlux
```


