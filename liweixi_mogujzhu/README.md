# Dataset
* Boston Fire Incident
https://data.boston.gov/dataset/fire-incident-reporting
* Boston Code Enforcement - Building and Property Violations:
https://data.boston.gov/dataset/code-enforcement-building-and-property-violations
* Boston Fire Alarm Boxes
http://bostonopendata-boston.opendata.arcgis.com/datasets/fire-alarm-boxes
* Boston Fire Department
http://bostonopendata-boston.opendata.arcgis.com/datasets/fire-departments
* Boston Fire Hydrants
http://bostonopendata-boston.opendata.arcgis.com/datasets/fire-hydrants
* Boston Weather
https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00014739/detail

# Package Dependency
* Pandas: Provide interface to calculate on the data.
  ```
    pip install pandas
  ```
  
# Data Document
## boston_fire_facility_transformation.py
* facility_type: The type of the facility.<br>
One of [fire_alarm_boxes, fire_hydrants, fire_department]
* coordinates: Coordinates of the facility. <br>
Shape: [latitude, longitude]

## weather_fire_incident_transformation.py
* DATE: The date
* TMAX: The maximum temperature
* TMIN: The minimum temperature
* TAVG: The average temperature
* AWND: Average daily wind speed
* PRCP: Precipitation
* SNOW: Snowfall
* SNWD: Snow depth
* NINCIDENT: Number of fire incident happened
* NLOSS: Number of fire incident which causes money loss