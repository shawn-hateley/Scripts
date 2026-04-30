// Import the modbus-serial library
const ModbusRTU = require('modbus-serial');

// Create a new Modbus client instance
const client = new ModbusRTU();

// Define connection parameters
const HOST = '10.10.122.198'; // Replace with the IP address of your Modbus server
const PORT = 502;             // Replace with the port number of your Modbus server
//const REGISTER_ADDRESS = 1;   // Replace with the address of the input register you want to read
var REGISTER_ADDRESS;   // Replace with the address of the input register you want to read

// Function to read input register
async function readInputRegister(REGISTER_ADDRESS) {
    try {
        // Connect to the Modbus server
        await client.connectTCP(HOST, { port: PORT });
        
        // Set the unit ID (typically 1 for Modbus TCP)
        client.setID(1);

        // Read the input register
        const response = await client.readInputRegisters(REGISTER_ADDRESS, 2);
        //console.log(response);
        // Extract the value from the response

		var buffer = new ArrayBuffer(4);
		var view = new DataView(buffer);

        //Set the endianness. CR1000 uses big endian by default
		view.setInt16(0, response.data[0], false);
		view.setInt16(2, response.data[1], false);
		var value = view.getFloat32(0, false).toFixed(2);

        
        // Print the value
        console.log(`Value of input register at address ${REGISTER_ADDRESS}: ${value}`);
		
    } catch (err) {
        // Handle errors
        console.error('Error reading input register:', err);
    } finally {
        // Close the connection
        client.close();
    }
}

// Execute the function
//readInputRegister(1);
readInputRegister(3);
//setInterval(readInputRegister, 1000);