// create an empty modbus client
var ModbusRTU = require("modbus-serial");
var client = new ModbusRTU();

// open connection to a tcp line
client.connectTCP("10.12.16.13", run);

// read the values of 10 registers starting at address 0
// on device number 1. and log the values to the console.
function run() {
    client.setID(1);

   // client.readInputRegisters(9219, 2)
   //     .then(console.log)
   //     .then(run);
	const response = client.readInputRegisters(9219, 2, () => {
	console.log(response);
	}).catch((err) => {
			console.error('Error:', err);
			client.destroy(); // Ensure the connection is closed on error
		});
	// Extract the value from the response

	//var buffer = new ArrayBuffer(4);
	//var view = new DataView(buffer);

	//view.setInt16(2, response.data[0], false);
	//view.setInt16(0, response.data[1], false);
	//var value = view.getFloat32(0, false).toFixed(2);

	
	// Print the value
	//console.log(`Value of input register at address ${REGISTER_ADDRESS}: ${value}`);
}

run();