var moment = require("moment");
var timeStamp = moment().format('YYYY-MM-DD_HH-mm-ss'); //generate a timestamp for the log file

var gpio = require('rpi-gpio');
gpio.setup(5, gpio.DIR_IN, gpio.EDGE_BOTH);
gpio.setMode(gpio.MODE_BCM);
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


gpio.on('change', function(channel, value) {
        //console.log('Channel ' + channel + ' value is now ' + value);
		delay(500);
        if (value != 0) {
                plug.setPowerState(true);
                console.log(timeStamp + 'Turning On');
                doorStatus = 1;
        } else {
                plug.setPowerState(false);
                console.log(timeStamp + 'Turning Off');
                doorStatus = 0;
        }
});