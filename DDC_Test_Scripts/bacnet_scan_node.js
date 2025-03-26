const Bacnet = require('bacstack');
const { EventEmitter } = require('events');

// Create a BACnet client
const bacnet = new Bacnet();

// Listen for I-Am responses (when a device responds to the Who-Is broadcast)
bacnet.on('iAm', (device) => {
    console.log('Device discovered:');
    console.log(`  Device ID: ${device.deviceId}`);
    console.log(`  Address: ${device.address}`);

    // Now we query the device for its object list
    discoverDeviceObjects(device);
});

// Discover devices by sending a Who-Is broadcast
function discoverDevices() {
    console.log('Sending Who-Is broadcast...');
    bacnet.whoIs(); // This sends a Who-Is request to the BACnet network
}

// Query the object list from a discovered device
function discoverDeviceObjects(device) {
    console.log(`Querying objects for device ID ${device.deviceId} at ${device.address}...`);

    // Request the object list from the device
    bacnet.readProperty(device.address, device.deviceId, Bacnet.enum.PropertyIdentifier.objectList, (err, value) => {
        if (err) {
            console.error(`Error reading object list for device ID ${device.deviceId}:`, err);
        } else {
            console.log(`Device ${device.deviceId} has the following objects:`);
            console.log(value);
        }
    });
}

// Start the discovery process
discoverDevices();

// Handle graceful shutdown on SIGINT (Ctrl+C)
process.on('SIGINT', () => {
    console.log('Shutting down BACnet scanner...');
    bacnet.close();
    process.exit();
});
