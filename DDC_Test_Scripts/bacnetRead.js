const bacnet = require('bacstack');

// BACnet device configuration
const deviceId = 400; // Replace with your device ID
const ipAddress = '10.10.1.47'; // Replace with your device IP address
const port = 47808; // BACnet default port

// Create a new BACnet client
const client = new bacnet();

// Read a BACnet variable
function readVariable(objectType, objectId, propertyId) {
  return new Promise((resolve, reject) => {
    client.readProperty(
      ipAddress,
      port,
      deviceId,
      objectType,
      objectId,
      propertyId,
      (err, value) => {
        if (err) {
          reject(err);
        } else {
          resolve(value);
        }
      }
    );
  });
}

// Example usage
(async () => {
  try {
    const result = await readVariable(
      bacnet.enum.ObjectType.analogInput,
      1,
      bacnet.enum.PropertyIdentifier.presentValue
    );
    console.log('Value:', result.values[0].value);
  } catch (error) {
    console.error('Error:', error);
  }
})();
