import pymodbus
import serial
import time
import os
import sys

from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient #initialize a serial $
from pymodbus.transaction import ModbusRtuFramer
from datetime import datetime
from statistics import mean
from statistics import stdev

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
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #Start the pump and start taking measurements.
    print('Starting Pump and Measurements')
    GPIO.output(PUMP_PIN,0)
    GPIO.output(SENSOR_PIN,1)

    #Print to file
    file = open(DATAFILE, "a")
    file.write('\n')
    file.write('Starting Measurement sequence\n')
    file.write('\n')
    file.write('%s ----Air Pump On----\n' % (currentTime))
    file.write('\n')
    file.close()
    #time.sleep(PUMP_TIME)

    #Start the measurement
    #GPIO.output(SENSOR_PIN,1)
    #time.sleep(SENSOR_TIME)

def recordData(client):

    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

    file = open(DATAFILE, "a")
    file.write("%s,%d,%d,%d\n" % (currentTime,co2Data,tempData,humData))
    file.close()

    return [co2Data, tempData, humData]

def endPump():
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #Stop pump
    print('Pump Off')
    GPIO.output(PUMP_PIN,1)

    #Print to file
    file = open(DATAFILE, "a")
    file.write('\n')
    file.write('%s ----Air Pump Off----\n' % (currentTime))
    file.write('\n')
    file.close()

def writeMean(co2Runtotal,tempRuntotal,rhRuntotal):
    print (round(mean(co2Runtotal),2))
    print (round(mean(tempRuntotal),2))
    print (round(mean(rhRuntotal),2))

    file = open(DATAFILE, "a")
    file.write('\n')
    file.write('CO2_Avg, CO2_StDev, Temp_Avg, Temp_StDev, RH_Avg, RH_StDev\n')
    file.write(' %s,%s,%s,%s,%s,%s\n' % (round(mean(co2Runtotal),2),round(stdev(co2Runtotal),2),round(mean(tempRuntotal),2),round(stdev(tempRuntotal),2),round(mean(rhRuntotal),2),round(stdev(rhRuntotal),2)))
    file.write('\n')
    file.close()


def endMeasurement():
    # Turn off pump and sensor
    GPIO.output(SENSOR_PIN,0)
    GPIO.output(PUMP_PIN,1)
    GPIO.cleanup()

def main(args):
    global PUMP_TIME
    global SENSOR_TIME
    co2Runtotal = []
    tempRuntotal = []
    rhRuntotal = []

    try:
        PUMP_TIME = int(args[1])
        SENSOR_TIME = int(args[2])
        print('using custom args. pump time: %d sensor time: %d' % (PUMP_TIME, SENSOR_TIME))
    except:
        PUMP_TIME = 600
        SENSOR_TIME = 300
        print('using default args. pump time: %d sensor time: %d' % (PUMP_TIME, SENSOR_TIME))

    if not os.path.isfile(DATAFILE_PATH):
        file = open(DATAFILE, "w")
        file.write('"TIMESTAMP","CO2","TEMPERATURE","HUMIDITY"\n"UTC","PPM","DEG_C","RH"\n')
        file.close()

    startMeasurement()

    # Connect to sensor and record data
    try:
        client = ModbusClient(method = "rtu", port="/dev/ttyS0",stopbits = 1, bytesize = 8, parity = 'N', baudrate= 9600)
        # Connect to the serial modbus server
        connection = client.connect()
        if connection:
            for x in range(0, PUMP_TIME, 30): #take a measurement every 30 secs until the pump and sensor time is over
                outputList = (recordData(client)) #return the co2, temp and rh data after every call and append it to list variables
                co2Runtotal.append(outputList[0])
                tempRuntotal.append(outputList[1])
                rhRuntotal.append(outputList[2])
                time.sleep(30)

            writeMean(co2Runtotal, tempRuntotal, rhRuntotal)
            endPump()

            co2Runtotal = []
            tempRuntotal = []
            rhRuntotal = []

            for x in range(0, SENSOR_TIME, 30):
                outputList = (recordData(client))
                co2Runtotal.append(outputList[0])
                tempRuntotal.append(outputList[1])
                rhRuntotal.append(outputList[2])
                time.sleep(30)

            writeMean(co2Runtotal, tempRuntotal, rhRuntotal)
        # Closes the underlying socket connection
        client.close()

    except Exception as error:
        print('CO2 measurement failed: ' + repr(error))
    finally:
        endMeasurement()

if __name__ == "__main__":
    main(sys.argv)
