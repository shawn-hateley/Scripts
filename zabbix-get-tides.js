var util = require("util");

var options = {
  "zabbix-server" : "10.12.1.28",
  "port" : "10051",
  "realtime" : true,
  "with-timestamps" : true,
  "verbose" : true
};
     
var zbx_sender = require('zbx_sender').createZabbixSender(options);

zbx_sender.on('data',function(resp,data){
  console.log('zbx_sender RESPONSE: '+JSON.stringify(resp));
});

zbx_sender.on('error',function(err,orig,data){
  console.log('zbx_sender Error: '+ err.message);
  orig && console.log('   Orig error: '+ util.inspect(orig));
  data && console.log('   On data: '+ util.inspect(data));
});

var https = require('https');
var options = {
  host: 'tides.server.hakai.app',
  path: '/tides/Heriot%20Bay?num_days=30'
};

var req = https.get(options, function(res) {
  //console.log('https.get STATUS: ' + res.statusCode);
  console.log('https.get HEADERS: ' + JSON.stringify(res.headers));

  // Buffer the body entirely for processing as a whole.
  var bodyChunks = [];
  res.on('data', function(chunk) {
    // You can process streamed parts here...
    bodyChunks.push(chunk);
  }).on('end', function() {
    var body = JSON.parse(Buffer.concat(bodyChunks).toString());
//    console.log('BODY:');
    let trap_data = [];
    // ...and/or process the entire body here.
    for (let i = 0; i < body.length; i++) {
      trap_data[i] = { "host": 'Marna Lab', "clock": Date.parse(body[i].time)/1000, "key" : 'heriot.bay.tides', "value":body[i].height };
    }
    var arrays = [], size = 250;
    
    for (let g = 0; g < trap_data.length; g += size){
      arrays.push(trap_data.slice(g, g + size));
    }
    for (let n = 0; n < arrays.length; n++) {
      setTimeout(function() {
        zbx_sender.send(arrays[n]);
      }, 200 * n);
    }
  })
});

req.on('error', function(e) {
  console.log('ERROR: ' + e.message);
});