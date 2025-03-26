#!/usr/bin/env python3

#Read holding registers and send result to Bacnet object.
# BACNet objects must exist on the target panel for this to work.

import csv
from datetime import datetime

import os
from dotenv import load_dotenv
load_dotenv()
#GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

from pyModbusTCP.client import ModbusClient
#SERVER_HOST = "192.168.1.170"
#SERVER_PORT = 502
SERVER_PORT = os.getenv('SERVER_PORT')

# BACnet address for Calvert Inverter DDC panel in Energy Center (Address 10200)
import BAC0
bacnet = BAC0.connect()
#BACNET_ADDR = "30100:192.168.1.64:47809"
BACNET_ADDR = os.getenv('BACNET_ADDR')



while(True):

    # Get ip addresses and bacnet object numbers from a csv file
    # Name, BACnet Analog Variable Address, Modbus IP address, unit ID, Register, Register Length
    # Laundry Room Inverter,801,192.168.1.160,3,30775,2

    with open('Modbus-bacnet-addresses.csv', newline='') as csvfile:
        data = list(csv.reader(csvfile))

        for x in data:
            SERVER_HOST = str(x[2])
            AV = str(x[1])
            ID = int(x[3])
            REGISTER = int(x[4])
            REG_LENGTH = int(x[5])

            #print(SERVER_HOST)

            # TCP auto connect on first modbus request
            c = ModbusClient(host=SERVER_HOST, port=SERVER_PORT, unit_id=ID , auto_open=True)

            regs = c.read_holding_registers(REGISTER, REG_LENGTH)

            # if success display registers
            if regs:
                r = BACNET_ADDR + ' analogValue ' + AV + ' presentValue ' + str(regs[1])
                bacnet.write(r)
                #bacnet.write('30100:192.168.1.64:47809 analogValue 800 presentValue' regs_l[1])
                #print(regs[1])
            else:
                now = datetime.now()
                print(now, ' unable to read register ',SERVER_HOST,' ',REGISTER)
