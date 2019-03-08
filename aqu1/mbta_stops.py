import json
import pandas as pd


reads = []
writes = ['aqu1.mbta_stops_data']

def mbta_stops(): 
    # Dataset 5: MBTA Bus Stops
    url = 'https://opendata.arcgis.com/datasets/2c00111621954fa08ff44283364bba70_0.csv?outSR=%7B%22wkid%22%3A102100%2C%22latestWkid%22%3A3857%7D'
    bus_stops = pd.read_csv(url)

    # Dataset 6: MBTA T stops 
    url = 'http://maps-massgis.opendata.arcgis.com/datasets/a9e4d01cbfae407fbf5afe67c5382fde_2.csv'
    t_stops = pd.read_csv(url)

    # Merge latitude and longitudes of all bus and T-stops in Boston
    bus = pd.concat([bus_stops.stop_lat, bus_stops.stop_lon], axis = 1) # select columns
    bus.columns = ['Latitude', 'Longitude']
    train = pd.concat([t_stops.Y, t_stops.X], axis = 1) # select columns
    train.columns = ['Latitude', 'Longitude']
    public_stops = bus.append(train) # aggregate data 
    public_stops = pd.DataFrame(public_stops)
    public_stops = json.loads(public_stops.to_json(orient = 'records'))
    return public_stops