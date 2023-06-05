// index.js
const express = require('express');
const bodyParser = require('body-parser');

var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: 'localhost'});

const app = express();
app.use(bodyParser.json());

function nestedLoop(obj) {
    const res = [];
    function recurse(obj, current) {
        for (const key in obj) {
            let value = obj[key];
            if(value != undefined) {
                if (value && typeof value === 'object') {
                   //console.log(`${key}`)
                    recurse(value, key);
                } else {
                   if (current != "metadata"){
                      let tmpVal = {"Name": current, "Value": value}// ${key}: ${value}}
                      res.push(tmpVal);
 
                   }
                }
            }
        }
    }
    recurse(obj);
    return res;
    console.log(res);
 }

app.post('/daily-rounds', (req, res) => {

    let data = req.body;
    console.log('Got body:', data);
    res.sendStatus(200);

 
    let processedData = nestedLoop(data);

    Sender.addItem('Facilities-Daily-Rounds','daily_rounds_raw', JSON.stringify(processedData));
    Sender.send(function(err, res) {
          if (err) {
            throw err;
          }
          console.dir(res);
    });

});

app.listen(8888, () => console.log(`Started server at http://localhost:8888!`));