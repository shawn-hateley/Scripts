import serial
import time
from datetime import datetime, timedelta

port = "COM" + input("Enter Comm Port ") or "1"
baud = input("Enter Baud Rate. (Default 115200) ") or "115200"
timeZone = input("Enter timezone offset in hours. (Default -7) ") or "-7"
ser = serial.Serial(port, baud)
ser.flushInput()
#f = open("test_data.csv","w")

def ddConvert(position, bearing):
	#4825.5649200,N
	#print(position)
	#print(bearing)
	degrees = int(position.split(".")[0][:-2])
	#print(degrees)
	minutes = float(position.split(str(degrees))[1]) / 60
	#print(minutes)
	if bearing == "S" or bearing == "W":
		return str(round((degrees + minutes) * -1,9))
	else:
		return str(round(degrees + minutes,9))

while True:

	ser_bytes = ser.readline()
	decoded_bytes = ser_bytes.decode("utf-8")
	
	#check for GGA line
	if decoded_bytes.find("$GNGGA") >= 0:
	#$GNGGA,233145.00,4825.5666554,N,12320.9755336,W,1,09,0.5,35.849,M,-19.924,M,0.0,*5F
		splitSentence = decoded_bytes.split(",")
		timeStamp = datetime.strptime(splitSentence[1], '%H%M%S.%f') + timedelta(hours=int(timeZone))
		#use datetime.replace() to combine todays date and the timeStamp time.
		today = datetime.now()
		timeStamp = timeStamp.replace(year=today.year, month=today.month, day=today.day)
		lat = splitSentence[2]
		latBearing = splitSentence[3]
		long = splitSentence[4]
		longBearing = splitSentence[5]
		# Decimal Degree conversion function
		latitude = ddConvert(lat,latBearing)
		longitude = ddConvert(long,longBearing)
		f = open("overlay_data.txt","w")
		g = open("gpsTrack_data.txt","a")
		#print(decoded_bytes)
		print(str(timeStamp) + ", " + latitude + ", " + longitude)
		f.write(str(timeStamp) + ", " + latitude + ", " + longitude)
		g.write(str(timeStamp) + ", " + latitude + ", " + longitude + "\n")
		f.close()
		g.close()
		

