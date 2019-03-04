# Download alerts for all of the MBTA in 20 day windows (the API max is 31 days).

# Only keep alerts which satisfy one of the following:
# 1) stop_id is within a certain number of miles of Boston
# 2) route_id is green, blue, orange, or red
# 3) it's a bus which operates within Boston

# You may want to clean up the alerts by alert version (only keep the last version?)

# You may want to do some string searching for "delay" - the "effect" field only offers
# "DETOUR"s, not delay info (shows up as "OTHER_EFFECT").

# May want to string search for "weather", "snow", "rain", or "wind"

# May want to also read in files "lines" and "routes" (in the stops script) to better 
# find relevant alerts ("informed_entity" has a route and trip ID, which you can use to filter 
# out to boston trains or something of the sort)

# ........

# download data in 20 day intervals for the whole year, merge it all together in memory, 
# filter out data not from the Blue, Red, Green, or Orange line, filter out alert versions so 
# we keep only the most recent alert version (via alert_id?), only then insert to Mongo

# maybe have a separate collection of all alerts due to weather and traffic (this script can 
# generate two collections - one of boston trains and one of traffic and weather related delays)
