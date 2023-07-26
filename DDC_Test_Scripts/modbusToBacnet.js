const Bacnet = require('bacstack');
const Modbus = require('jsmodbus');
const net = require('net');

// BACnet configuration
const bacnetClient = new Bacnet();
const bacnetDeviceId = 123; // Change this to your BACnet device ID

// Modbus configuration
const modbusHost = '192.168.1.161'; // Change this to your Modbus TCP host
const modbusPort = 502; // Change this to your Modbus TCP port
const modbusSlaveId = 3; // Change this to your Modbus slave ID

// Modbus register addresses
const modbusRegisterStart = 30775; // Change this to the starting register address
const modbusRegisterCount = 2; // Change this to the number of registers to read

// BACnet object and property IDs
const bacnetObjectId = { type: 0, instance: 1 }; // Change this to the desired BACnet object ID
const bacnetPropertyId = 85; // Change this to the desired BACnet property ID

// Create Modbus TCP client
const modbusClient = new Modbus.client.TCP(net.Socket);
const modbusConnectionOptions = {
  host: modbusHost,
  port: modbusPort
};

// Connect to Modbus TCP
modbusClient.connect(modbusConnectionOptions);

modbusClient.on('connect', () => {
  console.log('Connected to Modbus TCP');

  // Read Modbus registers and update BACnet points
  setInterval(() => {
    modbusClient.readHoldingRegisters(modbusRegisterStart, modbusRegisterCount, modbusSlaveId)
      .then((response) => {
        const values = response.response._body.valuesAsArray;
        console.log('Read Modbus registers:', values);

        // Convert Modbus values to BACnet format
        const bacnetValue = {
          type: Bacnet.enum.ApplicationTags.REAL,
          value: values[0] // Change this to the appropriate value index based on your Modbus register mapping
        };

        // Write BACnet value to the corresponding object property
        bacnetClient.writeProperty(
          { type: Bacnet.enum.ObjectTypes.OBJECT, instance: bacnetDeviceId },
          bacnetObjectId,
          bacnetPropertyId,
          bacnetValue,
          (error) => {
            if (error) {
              console.error('BACnet write error:', error);
            } else {
              console.log('BACnet write successful');
            }
          }
        );
      })
      .catch((error) => {
        console.error('Modbus read error:', error);
      });
  }, 5000); // Change the interval based on your requirements
});

// Handle Modbus TCP connection errors
modbusClient.on('error', (error) => {
  console.error('Modbus TCP connection error:', error);
});

// Handle BACnet events
bacnetClient.on('error', (error) => {
  console.error('BACnet error:', error);
});

bacnetClient.on('ready', () => {
  console.log('Connected to BACnet');
});
