#!/usr/bin/env python3

""" Read holding registers and send result to Bacnet object. """

import time
import csv

from pyModbusTCP.client import ModbusClient
#SERVER_HOST = "" 
SERVER_PORT = 502
ID = 3


import BAC0
bacnet = BAC0.connect()
BACNET_ADDR = "30100:192.168.1.64:47809"


with open('Modbus-bacnet-addresses.csv', newline='') as csvfile:
    data = list(csv.reader(csvfile))

    for x in data:
        SERVER_HOST = "\"" + x[2] + "\""
        AV = str(x[1])

        # TCP auto connect on first modbus request
        c = ModbusClient(host=SERVER_HOST, port=SERVER_PORT, unit_id=ID , auto_open=True)


        # main read loop
        #while True:
        # read registers at address  store result in regs list
        regs = c.read_holding_registers(30775, 2)

        # if success display registers
        if regs:
            r = BACNET_ADDR + ' analogValue ' + AV + 'presentValue ' + str(regs[1])
            bacnet.write(r)
            #bacnet.write('30100:192.168.1.64:47809 analogValue 800 presentValue' regs_l[1])
            print(regs[1])
        else:
            print('unable to read registers')

        # sleep 1s before next polling
        time.sleep(1)