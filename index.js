// index.js
const express = require('express');
const bodyParser = require('body-parser');

var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: 'it.hakai.org'});

const app = express();
//app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json())

app.post('/post-test', (req, res) => {
    res.json({requestBody: req.body})  // <==== req.body will be a parsed JSON object
    console.log('Got body:', req.body);
    Sender.addItem('Facilities Daily Rounds','daily_rounds_raw', req.body);
        Sender.send(function(err, res) {
          if (err) {
            throw err;
          }
          console.dir(res);
        });
   // res.sendStatus(200);
});

app.listen(8888, () => console.log(`Started server at http://localhost:8888!`));
