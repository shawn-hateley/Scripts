const net = require('net');
const { argv } = require('process'); // Access command-line arguments
const { setTimeout } = require('timers/promises');

// IP address and delay constants
const IPADDRESS = "10.10.8.25";
const DELAY = 5000; // milliseconds

// Get temperature from command-line arguments
const temperature = parseInt(argv[2], 10);

if (isNaN(temperature)) {
    console.error("Please provide a valid temperature value.");
    process.exit(1);
}

const client = new net.Socket();

client.connect(2101, IPADDRESS, () => {
    console.log("Beginning Transfer");

    // Example command to change value
    const checksum = (170 + 4 + 254 + 170 + 0 + temperature) & 255;
    const command = Buffer.from([170, 4, 254, 170, 0, temperature, checksum]);

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
