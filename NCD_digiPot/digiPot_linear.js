const net = require('net');
const { argv } = require('process'); // Access command-line arguments
const { setTimeout } = require('timers/promises');
//const fs = require('node:fs');
// Import the modbus-serial library
const ModbusRTU = require('modbus-serial');



// IP address and delay constants
const IPADDRESS = "10.10.8.25";
const DELAY = 500; // milliseconds

// Create a new Modbus client instance
const modbusClient = new ModbusRTU()
const modbusPort = 502
const modbusIP = "10.12.16.13" //Tank 3 Walchem
const tempRegister = 9219

// Get temperature from command-line arguments
const setPoint = parseInt(argv[2], 10);

if (isNaN(setPoint)) {
    console.error("Please provide a valid temperature value.");
    process.exit(1);
}

const dwyerSP = 20;
var setPointDiff = dwyerSP - setPoint;
//var currentTemp, tempDiff;



// Function to read input register
async function getModbusTemperature() {
    try {
        // Connect to the Modbus server

        await modbusClient.connectTCP(modbusIP, { port: modbusPort });

        // Set the unit ID (typically 1 for Modbus TCP)
        modbusClient.setID(1);

        // Read the input register
        const response = await modbusClient.readInputRegisters(tempRegister, 2);

        // Extract the value from the response

		var buffer = new ArrayBuffer(4);
		var view = new DataView(buffer);

		view.setInt16(2, response.data[0], false);
		view.setInt16(0, response.data[1], false);
		var value = view.getFloat32(0, false).toFixed(2);
		
        // Print the value
        console.log(`Value of input register at address ${tempRegister}: ${value}`);

		calculateNCDValue(value);
		
    } catch (err) {
        // Handle errors
        console.error('Error reading input register:', err);
    } finally {
        // Close the connection
        modbusClient.close();
    }
}


/* function getTemperature(){ //read the current tank temperature from a file
	try {
	  const temperature = fs.readFileSync(temperatureFile, 'utf8');
	  //console.log(temperature);
	  return (temperature);
	} catch (err) {
	  console.error(err);
	}

} */

function calculateNCDValue(currentTemp) {

	currentTemp = parseFloat(currentTemp);

	console.log("Current Temp = ", currentTemp)
	var tempDiff = setPoint - currentTemp;
	console.log("Current Setpoint = ", setPoint)

	var result = setPointDiff + currentTemp
	result = Math.round(3.27 * result + 32)
	console.log("NCD Command = ", result)

	changeTemp(result);
}

function changeTemp(dwyer){ //

	const client = new net.Socket();

	client.connect(2101, IPADDRESS, () => {
		console.log("Beginning Transfer");

		// Example command to change value
		const checksum = (170 + 4 + 254 + 170 + 0 + dwyer) & 255;
		const command = Buffer.from([170, 4, 254, 170, 0, dwyer, checksum]);

		client.write(command);

		setTimeout(DELAY).then(() => {
			client.once('data', (data) => {
				console.log("Transfer Complete");
				client.destroy(); // Close the connection
			});
		}).catch((err) => {
			console.error('Error:', err);
			client.destroy(); // Ensure the connection is closed on error
		});
	});
}


setInterval(getModbusTemperature, 10000);
