const bacnet = require('bacstack');

// Initialize BACStack
const client = new bacnet({adpuTimeout: 6000});

// Discover Devices
client.on('iAm', (device) => {
  console.log('address: ', device.address);
  console.log('deviceId: ', device.deviceId);
});
//client.whoIs();

// Read Device Object
/*  const requestArray = [{
  objectId: {type: 8, instance: 400},
  properties: [{id: 8}]
}];
client.readPropertyMultiple('10.10.126.29', requestArray, (err, value) => {
  console.log('value: ', value);
});  */

client.readProperty('10.10.126.29', {type: 0, instance: 1}, 85, (err, value) => {
  console.log('value: ', value.values[0].value);
});