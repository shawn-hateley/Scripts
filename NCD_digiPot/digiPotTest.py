#import socket object for TCP communications
import socket
import time
import sys

#change to IP address of the board
IPADDRESS = "10.10.8.25"
#change the time to delay between on and off commands
DELAY = 5
#change the total number of times the loop will run
#temperature = 100
temperature = int(sys.argv[1])

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((IPADDRESS, 2101))
print("Beginning Transfer")

# Example command to change value to 114
# 170 4 254 170 0 114 200 
checksum = (170 + 4 + 254 + 170 + 0 + temperature) & 255
command = bytearray([170, 4, 254, 170, 0, temperature, checksum])
print(checksum)
print(command)
s.send(command)
time.sleep(DELAY)
bback = s.recv(8)
print("Transfer Complete")
