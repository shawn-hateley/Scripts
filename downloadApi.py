# Example using hakai_api_client_python library
from hakai_api import Client
import sys, getopt


argv = sys.argv[1:]
station = ''
begin = ''
end = ''
variable = ''
all = ''
outputFile = ''

#parse arguments and assign to variables
try:
   opts, args = getopt.getopt(argv,"hs:b:e:v:a:f:",["station=","begin=","end=","variable=","all=","outputFile="])
except getopt.GetoptError:
   print 'test.py -s <stationName> -b <beginDate> -e <endDate> -v <variable> -a <all> -f <outputfile>'
   sys.exit(2)
for opt, arg in opts:
   if opt == '-h':
      print 'test.py -s <stationName> -b <beginDate> -e <endDate> -v <variableName>'
      sys.exit()
   elif opt in ("-s", "--stationName"):
      station = arg
   elif opt in ("-b", "--beginDate"):
      begin = arg
   elif opt in ("-e", "--endDate"):
      end = arg
   elif opt in ("-v", "--variableName"):
      variable = arg
   elif opt in ("-a", "--allData"):
      all = arg
   elif opt in ("-f", "--outputFileName"):
      outputFile = arg

if station == "": #check for station name
        print "Station name required (eg. -s SSN693DS)"
        sys.exit(2)

if begin != "": #check for begin date. If not present, no end date will be passed either
        begin = "?measurementTime>=" + begin

        if end != "":
                end = "&measurementTime<=" + end
else:
        end = ""

if all != "": #if present in command, get all data, otherwise just the first 20 records are returned from the api
        all = "&limit=-l"


client = Client()

url = "https://hecate.hakai.org/api/sn/views/" + station + ":5minuteSamples" + begin + end + all
print url

response = client.get(url)

data = response.json()
#check if a filter variable is present. If not, print all data
if variable != "":
        filter = station + ":" + variable
        f = open(outputFile, 'a')
        for i in data:
                output = str( i['measurementTime']) + "," + str(i[filter]) + "\n"
                f.write(output)
else:
        f = open(outputFile, 'a')
        f.write(str(data))


#use arguments to define the station, date and data
#https://hecate.hakai.org/api/sn/views/SSN693DS:5minuteSamples?measurementTime%3E=2016-01-01&measurementTime%3C=2016-02-01
#for station only need name SSN693DS and assume :5minuteSamples
#for date always ask for start and end
#for data if not filled out, return all. Need to think about the output format some more
