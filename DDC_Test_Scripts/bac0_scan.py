import BAC0
import time

def scan_for_devices():
    # Initialize BAC0
    bacnet = BAC0.lite()

    # Start BACnet discovery (Who-Is)
    print("Starting BACnet device discovery...")
    devices = bacnet.whois()

    if not devices:
        print("No BACnet devices found.")
    else:
        print(f"Found {len(devices)} BACnet devices:")
        for device in devices:
            print(f"  Device ID: {device.device_id} at {device.address}")
            # Query objects for each discovered device
            query_device_objects(bacnet, device)

    # Close the BACnet connection when done
    bacnet.close()

def query_device_objects(bacnet, device):
    print(f"  Querying objects for Device ID {device.device_id}...")
    
    try:
        # Request the object list for the device
        object_list = bacnet.read_property(device.address, device.device_id, "objectList")
        
        if object_list:
            print(f"    Device {device.device_id} has the following objects:")
            for obj in object_list:
                print(f"      - {obj}")
        else:
            print(f"    No objects found for Device ID {device.device_id}.")
    except Exception as e:
        print(f"  Error reading object list for device ID {device.device_id}: {e}")

if __name__ == "__main__":
    scan_for_devices()

