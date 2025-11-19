#!/usr/bin/env python3

""" Read holding registers and send result to Bacnet object. """

import time
from pyModbusTCP.client import ModbusClient

import BAC0
bacnet = BAC0.connect()

# TCP auto connect on first modbus request
c = ModbusClient(host="192.168.1.170", port=502, unit_id=3, auto_open=True)

# init modbus client
#c = ModbusClient(debug=False, auto_open=True)

# main read loop
while True:
    # read 10 registers at address 0, store result in regs list
    regs_l = c.read_holding_registers(30775, 2)

    # if success display registers
    if regs_l:
        r = '30100:192.168.1.64:47809 analogValue 800 presentValue ' + str(regs_l[1])
        bacnet.write(r)
        #bacnet.write('30100:192.168.1.64:47809 analogValue 800 presentValue' regs_l[1])
        print(regs_l[1])
    else:
        print('unable to read registers')

    # sleep 2s before next polling
    time.sleep(5)
