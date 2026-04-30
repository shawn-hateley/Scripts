const net = require('net');
const { argv } = require('process'); // Access command-line arguments
const { setTimeout } = require('timers/promises');

// IP address and delay constants
const IPADDRESS = "10.12.16.11";
const DELAY = 1000; // milliseconds

// Get temperature from command-line arguments

const temperatureIndex = argv.indexOf('-t');
const controllerIndex = argv.indexOf('-i');
const portIndex = argv.indexOf('-p');

if (controllerIndex > -1) {
  // Retrieve the value after --custom
  controllerAddress = process.argv[controllerIndex + 1];
} else {
    controllerAddress = "10.12.16.11"
}

const temperature = (parseInt(argv[temperatureIndex + 1], 10) || 97);
const controllerPort= (parseInt(argv[portIndex + 1], 10) || 1);

console.log(temperature);
console.log(controllerAddress);
console.log(controllerPort);

const client = new net.Socket();

client.connect(2101, controllerAddress, () => {
    console.log("Beginning Transfer");

    // Example command to change value
    const checksum = (170 + 4 + 254 + 170 + controllerPort + temperature) & 255;
    const command = Buffer.from([170, 4, 254, 170, controllerPort, temperature, checksum]);

    //console.log(checksum);
    //console.log(command);

    client.write(command);

    setTimeout(DELAY).then(() => {
        client.once('data', (data) => {
            console.log("Transfer Complete");
            var received = data.readInt16LE(2)
            console.log(received);
            if (received != 85){
                console.log("Failed")
            } else {
                console.log("Success")
            }
            client.destroy(); // Close the connection
        });
    }).catch((err) => {
        console.error('Error:', err);
        client.destroy(); // Ensure the connection is closed on error
    });
});
