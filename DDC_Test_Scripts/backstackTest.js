const bacnet = require('bacstack');

// Initialize BACStack
const client = new bacnet({adpuTimeout: 6000});

// Discover Devices
client.on('iAm', (device) => {
  console.log('address: ', device.address);
  console.log('deviceId: ', device.deviceId);
  console.log('maxAdpu: ', device.maxAdpu);
  console.log('segmentation: ', device.segmentation);
  console.log('vendorId: ', device.vendorId);
});
client.whoIs();

// Read Device Object
/* const requestArray = [{
  objectId: {type: 8, instance: 4194303},
  properties: [{id: 8}]
}];
client.readPropertyMultiple('192.168.1.43', requestArray, (err, value) => {
  console.log('value: ', value);
}); */