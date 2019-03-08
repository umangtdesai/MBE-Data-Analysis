import geopandas
import matplotlib

url = 'http://datamechanics.io/data/smithnj/CTA_RailStations.geojson'
data = geopandas.read_file(url)
print(list(data))
data.plot()