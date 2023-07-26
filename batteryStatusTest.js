// index.js
const express = require('express');
const bodyParser = require('body-parser');

const request = require('request');
let url = "http://192.168.1.95/custom/getBatteryBreakerStatus.php";
let options = {json: true};

var ZabbixSender = require('node-zabbix-sender');
var Sender = new ZabbixSender({host: '192.168.1.115'});

const app = express();
app.use(bodyParser.json());

// Get the JSON data from the battery monitoring page on the RevPi in the Energy Center
function getJSON(url, options){ 
    request(url, options, (error, res, body) => {
        if (error) {
            return  console.log(error)
        };

        if (!error && res.statusCode == 200) {
            // do something with JSON, using the 'body' variable
            //console.log(body)
            return nestedLoop(body);
        };
    });
    
}


//Transform the JSON data into a structure that Zabbix likes
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
                      let tmpVal = {"Cluster": current, "Battery":`${key}`, "Value": value}// ${key}: ${value}}
                      //console.log(`${key}`)
                      res.push(tmpVal);
 
                   }
                }
            }
        }
    }
    recurse(obj);
    console.log(res);
    //Send the transformed JSON data to a Zabbix trapper
    Sender.addItem('Energy Center','Battery_Room_Breaker_Raw', JSON.stringify(res));
    Sender.send(function(err, res) {
      if (err) {
        throw err;
      }
      console.dir(res);
    });
    return res;

 }


let processedData = getJSON(url, options); 
//console.log(processedData);










