#!/usr/bin/env python
# coding: utf8

## Script to get the times and height of low and high tides in a given day
## at a given place.
## Will work with Python 2.x with a slight change in the last line and
## removing '3' at end of the top line shebang

import sys
import requests
import json
from datetime import datetime, timedelta

f=open("/var/www/html/todaysTide.html", "w+")
output = []
pixelHeight = 8
maxTideHeight = 3


# Parameters, these should be command line arguments
#
TZ          = -7  # ADT, -8 for AST
date        = datetime.today().strftime("%Y-%m-%d")
stationID   = "5cebf1df3d0f4a073c4bbd1e"
timeSeriesCode = "wlp"

# One or the other of stationID or stationName must be given.
# The stationName does not have to be a perfect match to the actual name
# found in the Index of Sites: http://www.tides.gc.ca/eng/station/list
# For example 'shediac' will match 'Shediac Bay *'.

# Need UTC time for start and end of the date

start_dt = datetime.strptime(date + " 00:00:00", "%Y-%m-%d %H:%M:%S") - timedelta(hours=TZ)
end_dt = datetime.strptime(date + " 23:59:59", "%Y-%m-%d %H:%M:%S") - timedelta(hours=TZ)

# Convert the times to strings as needed for the search
sdt = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
edt =   end_dt.strftime("%Y-%m-%dT%H:%M:%S")

webPage = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/"

# The geographic coordinates in the search correspond to earth as a whole;
# the metadata station_id or stations_name will be used to select the area

# Using station_id metadata
url = webPage + stationID + "/data?time-series-code=" + timeSeriesCode + "&from=" + sdt + "Z&to=" + edt + "Z"


response = requests.get(url)
data = response.json()


## Print header as per CHS web page

# English header
print("Times and Heights for High and Low Tides")

# French header
#print("Heures et hauteurs des pleines et basses mers")

# get rid of * marking available observations
#print(where.replace("*", ""))
print(date)

# get only the hourly data and convert the tide height to pixel height
for x in range(len(data)):
        dt = datetime.strptime(data[x]["eventDate"], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=TZ)
        if dt.minute == 0:
                #print dt,
                #sys.stdout.write(",")
                #print(data[x]['value'])
                #print int(round((float(data[x]['value'])* pixelHeight/maxTideHeight)))
                output.append(int(round((float(data[x]['value'])* pixelHeight/maxTideHeight))))
#print data
print output
print >> f,output

f.close()