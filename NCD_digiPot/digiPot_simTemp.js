#!/usr/bin/env node
// A script to control the Dwyer temperature controller based on the current temperature and setpoint.

const net = require('net');
const { argv } = require('process'); // Access command-line arguments
const { setTimeout } = require('timers/promises');
//const fs = require('node:fs');
// Import the modbus-serial library
const ModbusRTU = require('modbus-serial');

//Handle command line arguments. Add more error checking for missing args. 
// Get temperature from command-line arguments
const setPoint = parseInt(argv[2]);

// Get tank number from command-line arguments
const tankNumber = parseInt(argv[3]);

if (isNaN(setPoint)) {
	console.error("Please provide a valid temperature value.");
	process.exit(1);
}

if (isNaN(tankNumber) || tankNumber > 40) {
	console.error("Please provide a valid tank number.");
	process.exit(1);
}


// IP address and delay constants
const IPADDRESS = "10.10.8.25";
const DELAY = 500; // milliseconds

// Create a new Modbus client instance
const modbusClient = new ModbusRTU()
const modbusPort = 502
//const modbusIP = "10.12.16.13" //Tank 3 Walchem
const temperatureRegister = 9219
const tankIP = tankNumber + 10;
const modbusIP = "10.12.254." + tankIP // Walchem IP

//Dwyer setpoints
const dwyerSP = 20;
var setPointDiff = dwyerSP - setPoint;


// Function to read input register
async function getModbusTemperature() {
    try {
        // Connect to the Modbus server

		console.log("Connecting to ", modbusIP)
		modbusClient.setTimeout(500); //time to wait for Walchem to respond

        await modbusClient.connectTCP(modbusIP, { port: modbusPort });
		
        // Set the unit ID (typically 1 for Modbus TCP)
        modbusClient.setID(1);

        // Read the input register
        const response = await modbusClient.readInputRegisters(temperatureRegister, 2);
		//console.log("Response ", response);
        // Extract the value from the response

		var buffer = new ArrayBuffer(4);
		var view = new DataView(buffer);

		view.setInt16(2, response.data[0], false);
		view.setInt16(0, response.data[1], false);
		var value = view.getFloat32(0, false).toFixed(2);
		
        // Print the value
        console.log(`Value of input register at address ${temperatureRegister}: ${value}`);

		calculateNCDValue(value);
		
    } catch (err) {
        // Handle errors
        console.error('Error reading from Walchem', err);
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
	var tempDiff = parseFloat(setPoint - currentTemp).toFixed(2); 
	console.log("Current Setpoint = ", setPoint)
	console.log("Current Temperature Difference = ", tempDiff)

	var NCDCommand = setPointDiff + currentTemp
	NCDCommand = Math.round(3.27 * NCDCommand + 32)
	console.log("NCD Command = ", NCDCommand)

	changeTemp(NCDCommand);
}

function changeTemp(NCDCommand){ //

	const client = new net.Socket();

	client.connect(2101, IPADDRESS, () => {
		console.log("Beginning Transfer");

		// NCD Ccmmand to change value. tank number may need to change depending on the setup of the digital potentiometers
		const checksum = (170 + 4 + 254 + 170 + tankNumber -1 + NCDCommand) & 255;
		const command = Buffer.from([170, 4, 254, 170, tankNumber -1, NCDCommand, checksum]);

		client.write(command);

		setTimeout(DELAY).then(() => {
			client.once('data', (data) => {
				if (data.readInt16LE(2) == 85){
					console.log("Transfer Complete");
					console.log(""); //newline
				}
				client.destroy(); // Close the connection
			});
		}).catch((err) => {
			console.error('Error:', err);
			client.destroy(); // Ensure the connection is closed on error
		});
	});
}


setInterval(getModbusTemperature, 5000);
