import pymodbus
import serial
import time
import os
import sys
from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient #initialize a serial $
from pymodbus.transaction import ModbusRtuFramer
from datetime import datetime

#import logging
#logging.basicConfig()
#log = logging.getLogger()
#log.setLevel(logging.DEBUG)

import RPi.GPIO as GPIO

CWD = os.getcwd()
DATAFILE = "quadraco2.txt"
DATAFILE_PATH = os.path.join(CWD, DATAFILE)

PUMP_PIN = 4
SENSOR_PIN = 17
# PUMP_TIME = 3
# SENSOR_TIME = 5

GPIO.setmode(GPIO.BCM)
GPIO.setup(SENSOR_PIN,GPIO.OUT,initial=GPIO.LOW)
GPIO.setup(PUMP_PIN,GPIO.OUT, initial=GPIO.HIGH)

def startMeasurement():
    #Start the pump
    GPIO.output(PUMP_PIN,0)
    time.sleep(PUMP_TIME)

    #Start the measurement
    GPIO.output(SENSOR_PIN,1)
    time.sleep(SENSOR_TIME)

def recordData(client):

    currentTime = datetime.now()
    #Starting add, num of reg to read, slave unit.
    co2= client.read_input_registers(0x0003,1,unit= 0xFE)
    temperature= client.read_input_registers(0x0004,1,unit= 0xFE)
    humidity= client.read_input_registers(0x0005,1,unit= 0xFE)

    co2Data = co2.registers[0]
    tempData = temperature.registers[0]/100
    humData = humidity.registers[0]/100

    print "CO2 ",co2Data
    print "Temperature ",tempData
    print "Humidity ",humData

    if not os.path.isfile(DATAFILE_PATH):
        file = open(DATAFILE, "w")
        file.write('"TIMESTAMP","CO2","TEMPERATURE","HUMIDITY"\n"UTC","PPM","DEG_C","?"\n')
        file.close()

    file = open(DATAFILE, "a")
    file.write("%s,%d,%d,%d\n" % (currentTime,co2Data,tempData,humData))
    file.close()

def endMeasurement():
    # Turn off pump and sensor
    GPIO.output(SENSOR_PIN,0)
    GPIO.output(PUMP_PIN,1)
    GPIO.cleanup()

def main(args):
    global PUMP_TIME
    global SENSOR_TIME
    try:
        PUMP_TIME = int(args[1])
        SENSOR_TIME = int(args[2])
        print('using custom args. pump time: %d sensor time: %d' % (PUMP_TIME, SENSOR_TIME))
    except:
        PUMP_TIME = 3
        SENSOR_TIME = 5
        print('using default args. pump time: %d sensor time: %d' % (PUMP_TIME, SENSOR_TIME))

    startMeasurement()

    # Connect to sensor and record data
    try:
        client = ModbusClient(method = "rtu", port="/dev/ttyS0",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600)
        # Connect to the serial modbus server
        connection = client.connect()
        if connection:
            recordData(client)
        # Closes the underlying socket connection
        client.close()

    except Exception as error:
        print('CO2 measurement failed: ' + repr(error))
    finally:
        endMeasurement()

if __name__ == "__main__":
    main(sys.argv)

