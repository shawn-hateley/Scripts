process.on('unhandledRejection', (err) => {
    //console.error('Unhandled Promise Rejection:', err);
});

process.on('uncaughtException', (err) => {
    if (err.code === 'ECONNRESET') {
        console.log('Uncaught ECONNRESET (ignored)');
        return;
    }
    console.error('Uncaught Exception:', err);
});

const net = require('net');
const { argv } = require('process'); // Access command-line arguments
const { setTimeout } = require('timers/promises');
//const fs = require('node:fs');
// Import the modbus-serial library
const ModbusRTU = require('modbus-serial');



// IP address and delay constants
//const IPADDRESS = "10.12.16.11";
const DELAY = 500; // milliseconds

// Create a new Modbus client instance

const modbusPort = 502
//const modbusIP = "10.12.254.33" //Tank 23 Walchem
const tempRegister = 9219

let controllerAddress;
let walchemAddress;
let setPoint;
let controllerPort;

// Get temperature from command-line arguments

const setPointIndex = argv.indexOf('-t');
const controllerIndex = argv.indexOf('-i');
const portIndex = argv.indexOf('-p');
const walchemIndex = argv.indexOf('-w');
const helpIndex = argv.indexOf('-h');

if (helpIndex > -1) {
  // Retrieve the value after --custom
	console.log("You must enter the temperature and the Walchem IP address for this script to run properly. \n-t Temperature, \n-w Walchem IP Address, \n-i NCD Controller IP Address (default = 10.12.16.11), \n-p NCD Controller Port Number (default = 1)")
	process.exit(1);
}

if (setPointIndex > -1) {
  // Retrieve the value after --custom
   setPoint = parseFloat(argv[setPointIndex + 1], 10);
} else {
   console.log("No setpoint provided, using 20 as default")
   setPoint = 20;//
}

if (controllerIndex > -1) {
  // Retrieve the value after --custom
   controllerAddress = process.argv[controllerIndex + 1];
} else {
   controllerAddress = "10.12.16.11"
}

if (portIndex > -1) {
  // Retrieve the value after --custom
   controllerPort = parseInt(argv[portIndex + 1], 10);
} else {
   console.log("No port provided, using 1 as default")
   controllerPort = 1;//
}

if (walchemIndex > -1) {
  // Retrieve the value after --custom
	walchemAddress = process.argv[walchemIndex + 1];
} else {
    console.log("Please provide a valid Walchem IP address")
	process.exit(1);
}



//const setPoint = (parseInt(argv[setPointIndex + 1], 10) || 20);
//const controllerPort= (parseInt(argv[portIndex + 1], 10) || 1);

const dwyerSP = 20;
var setPointDiff = dwyerSP - setPoint;
//var currentTemp, tempDiff;
//console.log(setPoint);
//console.log(controllerAddress);
//console.log(walchemAddress);


// Function to read input register
async function getModbusTemperature() {
	const modbusClient = new ModbusRTU()


    modbusClient.on("error", (err) => {
        if (err.code === "ECONNRESET") {
            console.log("Modbus socket reset (normal for this device)");
            return;
        }
        console.error("Modbus socket error:", err);
    });

    try {
        // Connect to the Modbus server

        await modbusClient.connectTCP(walchemAddress, { port: modbusPort });

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
//       console.log(`Value of input register at address ${tempRegister}: ${value}`);
		// Close the connection
//		console.log("closing modbus connection")
//		modbusClient.close();

		calculateNCDValue(value);
		
    } catch (err) {
        // Handle errors
        if (err.code === "ECONNRESET") {
            console.log("Modbus reset (device closed connection)");
        } else {
            console.error("Error reading input register:", err);
        }

    } finally {
        // Close the connection
		try {
            modbusClient.close();
        } catch {}
    }
}



function calculateNCDValue(currentTemp) {

	currentTemp = parseFloat(currentTemp);

	console.log("Current Temp = ", currentTemp)
	var tempDiff = setPoint - currentTemp;
	console.log("Current Setpoint = ", setPoint)

	var result = setPointDiff + currentTemp
	console.log("Dwyer Temp set to ", result)
	result = Math.round(3.21 * result + 39.4)
	console.log("NCD Command = ", result)

	changeTemp(result);
}

async function changeTemp(dwyer){ //
	const client = new net.Socket();

	try {

		client.connect(2101, controllerAddress, () => {
			console.log("Beginning Transfer");

			// Example command to change value
			const checksum = (170 + 4 + 254 + 170 + controllerPort + dwyer) & 255;
			const command = Buffer.from([170, 4, 254, 170, controllerPort, dwyer, checksum]);

			client.write(command);
			
		});

		client.on('data', (data) => {
//			console.log("Transfer Complete");
			var received = data.readInt16LE(2)
//			console.log(received);
			if (received != 85){
				console.log("Failed")
			} else {
				console.log("Success")
			}
			client.end(); // Close the connection
		});

		// Handle connection closure
		client.on('close', () => {
//			console.log('Connection closed');
		});

		// Handle errors
		client.on('error', (err) => {
			if (err.code === 'ECONNRESET') return;
			client.destroy(); // Ensure the connection is closed on error
		});

	} catch (error){
		console.error(`Error: ${error.message}`);
		client.destroy(); // Ensure the connection is closed on error
	}
}


setInterval(getModbusTemperature, 10000);
