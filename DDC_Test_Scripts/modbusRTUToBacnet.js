const Bacnet = require('bacstack');
const ModbusRTU = require('modbus-serial');

// BACnet configuration
const bacnetClient = new Bacnet();
const bacnetDeviceId = 1237; // Change this to your BACnet device ID

// Modbus configuration
const modbusClient = new ModbusRTU();
const modbusPort = '/dev/ttyUSB0'; // Change this to your Modbus serial port
const modbusSlaveId = 1; // Change this to your Modbus slave ID

// Modbus register addresses
const modbusRegisterStart = 100; // Change this to the starting register address
const modbusRegisterCount = 10; // Change this to the number of registers to read

// BACnet object and property IDs
const bacnetObjectId = { type: 0, instance: 1 }; // Change this to the desired BACnet object ID
const bacnetPropertyId = 85; // Change this to the desired BACnet property ID

// Connect to Modbus
modbusClient.connectRTU(modbusPort, { baudRate: 9600 }, () => {
  modbusClient.setID(modbusSlaveId);
  console.log('Connected to Modbus');

  // Read Modbus registers and update BACnet points
  setInterval(() => {
    modbusClient.readHoldingRegisters(modbusRegisterStart, modbusRegisterCount, (error, data) => {
      if (error) {
        console.error('Modbus read error:', error);
      } else {
        const values = data.data;
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
      }
    });
  }, 5000); // Change the interval based on your requirements
});

// Handle BACnet events
bacnetClient.on('error', (error) => {
  console.error('BACnet error:', error);
});

bacnetClient.on('ready', () => {
  console.log('Connected to BACnet');
});
