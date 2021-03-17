var moment = require("moment");
var timeStamp = moment().format('YYYY-MM-DD_HH-mm-ss'); //generate a timestamp for the log file

var Gpio = require('onoff').Gpio; //include onoff to interact with the GPIO
var pushButton = new Gpio(5, 'in', 'both',{debounceTimeout: 50}); //use GPIO pin 5 as input, and 'both' button presses, and releases should be handled
var doorStatus = 0;

const { Client } = require('tplink-smarthome-api');
const client = new Client();
const plug = client.getPlug({ host: '192.168.1.82' });

const http = require('http');
http.createServer(function (request, response) {
  response.writeHead(200, {"Content-Type": "text/html"});
  response.write('Door Status: ');
  response.write(doorStatus.toString());
  response.end();
}).listen(8090); //the server object listens on port 8090


pushButton.watch(function (err, value) { //Watch for hardware interrupts on pushButton GPIO, specify callback function
  if (err) { //if an error
    console.error(timeStamp + 'There was an error', err); //output error message to console
  return;
  }
  if (value != 0) {
                plug.setPowerState(true);
                console.log(timeStamp + 'Turning On');
                doorStatus = 1;
  } else {
                plug.setPowerState(false);
                console.log(timeStamp + 'Turning Off');
                doorStatus = 0;
  }
  //setTimeout(function(){
  //      plug.getPowerState() //check door state and confirm light state.
  //});
});

pushButton.read((err,value) => {
        if (err) {
                throw err;
        }
        console.log(value);
});

function unexportOnClose() { //function to run when exiting program
  pushButton.unexport(); // Unexport Button GPIO to free resources
};

process.on('SIGINT', unexportOnClose); //function to run when user closes using ctrl+c
