'use strict';

const fs = require('fs');
const { networkInterfaces } = require('os');

const nets = networkInterfaces();

var ipAddresses;
var results;

//fs.writeFileSync('/tmp/ipAddress.txt',"");

fs.readFile('/tmp/ipAddress.txt', 'utf8', (err, ipAddresses) => {
    if (err) {
      console.error('Error reading file:', err);
      return;
    }
    console.log('File contents:', ipAddresses);
});
//console.log(ipAddresses);

for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
        // Skip over non-IPv4 and internal (i.e. 127.0.0.1) addresses
        // 'IPv4' is in Node <= 17, from 18 it's a number 4 or 6
        const familyV4Value = typeof net.family === 'string' ? 'IPv4' : 4
        if (net.family === familyV4Value && !net.internal) {

            results = name + " " + net.address + "\n";

            fs.appendFile('/tmp/ipAddress.txt',results, function(err) {
                   if(err) {
                       return console.log(err);
                   }
                   //console.log("The file was saved!");
               }); 
            console.log(results);
        }
    }
}

