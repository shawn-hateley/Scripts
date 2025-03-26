const fs = require('fs');
const csv = require('csv-parser');
var util = require("util");

var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: '192.168.1.115'});


const inputFilePath = 'input.csv'; // Path to your input CSV file
const outputFilePath = 'output.json'; // Path to your output JSON file

const results = [];

// Read the CSV file
fs.createReadStream(inputFilePath)
  .pipe(csv())
  .on('data', (data) => results.push(data))
  .on('end', () => {
    // Write the JSON output to a file
	  //console.log(JSON.stringify(results, null, 2))
    //Sender.send(JSON.stringify(results, null, 2));
    Sender.addItem('1015_AutoSalt','autosalt_doseevent', JSON.stringify(results, null, 2));
    Sender.send(function(err, results) {
      if (err) {
        throw err;
      }
      console.dir(results);
    });
    // fs.writeFile(outputFilePath, JSON.stringify(results, null, 2), (err) => {
    //   if (err) {
    //     console.error('Error writing JSON file:', err);
    //   } else {
    //     console.log('CSV successfully converted to JSON and saved to', outputFilePath);
    //   }
    // });
  });



