#!/usr/bin/env python3

#Read holding registers and send result to Bacnet object.
# BACNet objects must exist on the target panel for this to work.
from datetime import datetime

""" How-to add float support to ModbusClient. """

from pyModbusTCP.client import ModbusClient
from pyModbusTCP.utils import (decode_ieee, encode_ieee, long_list_to_word,
                               word_list_to_long)

from pyModbusTCP.client import ModbusClient
SERVER_HOST = "10.12.254.19"
SERVER_PORT = 502
ID = 1
REGISTER = 9219
REG_LENGTH = 2



class FloatModbusClient(ModbusClient):
    """A ModbusClient class with float support."""

    def read_float(self, address, number=1):
        """Read float(s) with read holding registers."""
        reg_l = self.read_holding_registers(address, number * 2)
        if reg_l:
            return [decode_ieee(f) for f in word_list_to_long(reg_l)]
        else:
            return None

    def write_float(self, address, floats_list):
        """Write float(s) with write multiple registers."""
        b32_l = [encode_ieee(f) for f in floats_list]
        b16_l = long_list_to_word(b32_l)
        return self.write_multiple_registers(address, b16_l)
    


# TCP auto connect on first modbus request
c = FloatModbusClient(host=SERVER_HOST, port=SERVER_PORT, unit_id=ID , auto_open=True)

regs = c.read_holding_registers(REGISTER, REG_LENGTH)
float_l = c.read_float(REGISTER, REG_LENGTH)

# if success display registers
if regs:

    print(regs)
    print(float_l)

else:
    now = datetime.now()
    print(now, ' unable to read register ',SERVER_HOST,' ',REGISTER)

