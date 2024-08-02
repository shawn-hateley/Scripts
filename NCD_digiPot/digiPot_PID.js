const net = require('net');
const { argv } = require('process'); // Access command-line arguments
const { setTimeout } = require('timers/promises');
const fs = require('node:fs');

// IP address and delay constants
const IPADDRESS = "10.10.8.25";
const DELAY = 500; // milliseconds

// Get temperature from command-line arguments
const dwyer = parseInt(argv[2], 10);

if (isNaN(dwyer)) {
    console.error("Please provide a valid temperature value.");
    process.exit(1);
}

const setPoint = 25;
const temperatureFile = "C:\\Users\\Shawn\\Documents\\Projects\\Scripts\\NCD_digiPot\\temperature.txt"
//const temperature = 0;
let Controller = require('node-pid-controller');
 
let ctr = new Controller({
  k_p: 0.25,
  k_i: 0.01,
  k_d: 0.01,
  dt: 10
});
ctr.setTarget(setPoint); // 25 degrees


function getTemperature(){
	try {
	  const temperature = fs.readFileSync(temperatureFile, 'utf8');
	  //console.log(temperature);
	  return (temperature);
	} catch (err) {
	  console.error(err);
	}

}

function changeTemp(dwyer){
	const client = new net.Socket();

	client.connect(2101, IPADDRESS, () => {
		console.log("Beginning Transfer");

		// Example command to change value
		const checksum = (170 + 4 + 254 + 170 + 0 + dwyer) & 255;
		const command = Buffer.from([170, 4, 254, 170, 0, dwyer, checksum]);

		//console.log(checksum);
		//console.log(command);

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


//PID Loop

let correction = ctr.update(getTemperature()); // 20 degrees is the current temp

let goalReached = false
while (!goalReached) {
  let output = getTemperature();
  console.log(output);
  let input  = ctr.update(output);
  console.log(input);
  //Use the input value(PID multiplier) to control the amount of time the dwyer is active heating or cooling. The larger the input value, 
  // the longer the time the dwyer is active. If the PID value is positive, we are heating, if it is negative, then we are cooling. 
  //changeTemp(input);

  goalReached = (input === 0) ? true : false; // in the case of continuous control, you let this variable 'false'
}

//changeTemp(dwyer)
