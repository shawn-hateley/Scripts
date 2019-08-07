# wiring notes
# connect a jumper wire between pin 17 and the right hand jumper pin on the co2 board. The co2 measurement is
# activated by setting the pin high (sending it 3V)

# pin 4 controls the pump

import pymodbus
import serial
from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient #initialize a serial RTU client instance
from pymodbus.transaction import ModbusRtuFramer

import time

import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

import RPi.GPIO as GPIO
GPIO.setmode(GPIO.BCM)
GPIO.setup(17,GPIO.OUT,initial=GPIO.LOW)
GPIO.setup(4,GPIO.OUT, initial=GPIO.HIGH)


#Start the pump
#GPIO.output(4,0)

#time.sleep(10)

#Start the measurement
#GPIO.output(17,1)

time.sleep(2)

#count= the number of registers to read
#unit= the slave unit this request is targeting
#address= the starting address to read from

client= ModbusClient(method = "rtu", port="/dev/ttyS0",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600)
unit = 0xFE

#Connect to the serial modbus server
connection = client.connect()


#print connection
if connection:
	print "Getting Sensor Data"

#Starting add, num of reg to read, slave unit.
#co2= client.read_input_registers(0x0003,1,unit= 0xFE)
#temperature= client.read_input_registers(0x0004,1,unit= 0xFE)
#humidity= client.read_input_registers(0x0005,1,unit= 0xFE)

client.write_register(0x0000,0,unit=unit)
client.write_register(0x0001,0x7C06,unit=unit)
#client.write_registers(0x1f,0xb4,unit= 0xFE)
time.sleep(10)
calResponse = client.read_holding_registers(0x0000,1,unit= unit)

#result= client.read_holding_registers(0x001f,8,unit= 0xFE)
#client.write_register(0x001f,0x00B4,unit=unit)
#device=client.read_holding_registers(0x001f,1,unit=unit)
#print device.registers[0]

#print "CO2 ",co2.registers[0]
#print "Temperature ",(temperature.registers[0]/100)
#print "Humidity ",(humidity.registers[0]/100)
for x in calResponse.registers:
	print "Calibration Response ",x

#test = map(int, co2.registers)
#test = temperature.registers[0]/100
#print test
#print result
#Closes the underlying socket connection
client.close()

GPIO.output(17,0)
GPIO.output(4,1)
