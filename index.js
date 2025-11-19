// index.js
const express = require('express');
const bodyParser = require('body-parser');

var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: 'localhost'});

const app = express();
app.use(bodyParser.json());

app.post('/daily-rounds', (req, res) => {

    let data = req.body;
    console.log('Got body:', data);
    Sender.addItem('Facilities-Daily-Rounds','daily_rounds_raw', JSON.stringify(data));
    Sender.send(function(err, res) {
          if (err) {
            throw err;
          }
          console.dir(res);
    });

});

app.listen(8888, () => console.log(`Started server at http://localhost:8888!`));