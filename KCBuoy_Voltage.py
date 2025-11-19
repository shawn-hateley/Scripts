from hakai_api import Client

from dotenv import load_dotenv
envPath = "/home/hakai/zabbix_scripts/.env"
load_dotenv(dotenv_path=envPath)

import os
token = os.environ.get("CREDENTIAL_TOKEN")

from zabbix_utils import Sender

from datetime import datetime, timedelta

#print tehe datetime that the script is run
print()
print("Script run at",datetime.now())
print()

# Pass a credentials token as the Client Class is initiated
client = Client(credentials=token)

# Make a data request for chlorophyll data
url = '%s/%s' % (client.api_root, 'sn/views/KCBuoy:Diagnostics?sort=-measurementTime')
response = client.get(url)

y = response.json() #convert to json

for i in range(0,12):
        #get data from json and convert time to unix timestamp
        time = datetime.strptime((y[i]["measurementTime"]),'%Y-%m-%dT%H:%M:%S.%fZ')
        time = time - timedelta(hours=8) #convert from Zulu to PST
        epochTime = time.timestamp() #convert to epoch time
        voltage = (y[i]["KCBuoy:BattVolt_Avg"])

        #send data to zabbix
        sender = Sender(server='127.0.0.1', port=10051)
        zabbix_response = sender.send_value('KCBuoy', 'kcbuoy.voltage', voltage, epochTime)

        print(time)
        print(voltage)

        #print(zabbix_response)
        # {"processed": 1, "failed": 0, "total": 1, "time": "0.000338", "chunk": 1}