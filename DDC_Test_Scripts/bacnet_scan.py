import logging
from bacpypes.core import run, stop
from bacpypes.pdu import Address
from bacpypes.app import Application
from bacpypes.object import get_object_class
from bacpypes.device import LocalDeviceObject
from bacpypes.service.device import WhoIsIAmServices
from bacpypes.discovery import WhoIsRequest, IAmRequest

# Enable logging for debugging
logging.basicConfig(level=logging.DEBUG)

class BacnetScanner(Application):
    def __init__(self, device, network):
        # Initialize the BACnet application with device and network details
        self.device = device
        self.network = network

        # Initialize the Application
        Application.__init__(self, device)

    def scan(self):
        # Send a Who-Is request to find BACnet devices
        who_is_request = WhoIsRequest()
        self.send(who_is_request)

    def doIAm(self, iAmRequest):
        # This method is called when an I-Am response is received
        print(f"Device ID: {iAmRequest.device_id}, Address: {iAmRequest.address}")
        
        # Request to list objects on the device
        self.list_device_objects(iAmRequest.device_id, iAmRequest.address)

    def list_device_objects(self, device_id, address):
        # For the sake of simplicity, let's assume we request a Device object and print available properties
        print(f"Listing objects for Device ID {device_id} at {address}...")
        device_obj = get_object_class("device")(device_id)
        
        # You can extend this to fetch more objects and properties
        print(f"Device {device_id} Object: {device_obj}")

# Define a local device and the network address for the BACnet scan
local_device = LocalDeviceObject( device_id=123, object_name="ScannerDevice", object_type="device" )
network_address = Address("192.168.1.255")  # Broadcast address or a specific BACnet network address

# Instantiate the scanner application
scanner = BacnetScanner(local_device, network_address)

# Start scanning
scanner.scan()

# Run the application
run()

