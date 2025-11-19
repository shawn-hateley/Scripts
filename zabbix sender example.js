var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: 'it.hakai.org'});

const http = require('http');
var CronJob = require('cron').CronJob;
var job = new CronJob(
  '*/2 * * * *',
  function() {
    http.get('http://10.83.8.10/json', (resp) => {
      let data = '';
      resp.on('data', (chunk) => {
        data += chunk;
      });
      resp.on('end', () => {
        console.log(JSON.parse(data));
        Sender.addItem('PurpleAirTest1','purple.air.json', data);
        Sender.send(function(err, res) {
          if (err) {
            throw err;
          }
          console.dir(res);
        });
      });
    }).on("error", (err) => {
      console.log("Error: " + err.message);
    });
  },
  null,
  true,
  'America/Los_Angeles'
);