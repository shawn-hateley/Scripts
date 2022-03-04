#!/usr/bin/python
# -*- coding: utf-8 -*-
# version 1.0.0
from pprint import pprint
import os
import sys, getopt
import base64
import json
import requests
from dotenv import load_dotenv
load_dotenv()

# add arguments from command line for device id
def getArgs(argv):
    global outputFile
    global deviceId
    deviceId = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print ('swarm_downloader.py -i <deviceId> -o <outputFile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ('swarm_downloader.py -i <deviceId> -o <outputFile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            deviceId = arg
        elif opt in ("-o", "--outputFile"):
            outputFile = arg


if __name__ == "__main__": #call the get arguments function when the script is run directly, and not as a module
   getArgs(sys.argv[1:])


# define output of the REST request as json
# and other parameterized values used below
loginHeaders = {'Content-Type': 'application/x-www-form-urlencoded'}
hdrs = {'Accept': 'application/json'}
#put the .env file with the username and password in the same directory as the script. Somewhere else to be determined
username = os.environ.get('SWARM_USER_NAME') or 'hakai-sensor-network'
password = os.environ.get('SWARM_PW')
loginParams = {'username': username, 'password': password}

hiveBaseURL = 'https://bumblebee.hive.swarm.space/hive'
loginURL = hiveBaseURL + '/login'
getMessageURL = hiveBaseURL + '/api/v1/messages'
ackMessageURL = hiveBaseURL + '/api/v1/messages/rxack/{}'

# dont do the ACK
doACK = True

# create a session
with requests.Session() as s:
    # log in to get the JSESSIONID cookie
    res = s.post(loginURL, data=loginParams, headers=loginHeaders)
    #print(res.url)

if res.status_code != 200:
    print("Invalid username or password; please use a valid username and password in loginParams.")
    exit(1)

# print out the JSESSIONID cookie
#print(s.cookies)

# let the session manage the cookie and get the output for the given appID
# only pull the last 10 items that have not been ACK'd
res = s.get(getMessageURL, headers=hdrs, params={'deviceid':deviceId, 'count': 50, 'status': 0})
# print(res)


messages = res.json()

if not messages: #check if any new messages downloaded and exit if none
    print("No data found")
    exit(1)
# print out the prettied version of the JSON records for unacknowledge data records in the hive
#print(json.dumps(messages, indent=4))

counter = 0 #used to count the number of messages returned and acknowledge them
# for all the items in the json returned (limited to 10 above)
for item in messages:
    # if there is a 'data' keypair, output the data portion converting it from base64 to ascii - assumes not binary
    if (item['data']):
        #print('Decoded Message: ' + base64.b64decode(item['data']).decode('ascii') + '\n')
        print(base64.b64decode(item['data']).decode('ascii') + '\n')
        f = open("SGTEUS.csv", "a")
        f.write(base64.b64decode(item['data']).decode('ascii') + '\n')
        f.close()

        # example ACK the first item returned ONLY!
        if (doACK):
            # note you should check the initial return to make sure there are packets available before you ack one
            res1 = s.post(ackMessageURL.format(messages[counter]['packetId']), headers=hdrs)

            # print out the response from the ACK request
            pprint(res1.json())
    counter +=1
