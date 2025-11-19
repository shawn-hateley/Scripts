// Create express app
const fs = require('fs');
const express = require("express");
const serveIndex = require('serve-index');
const app = express();
const bodyParser = require('body-parser');
const moment = require('moment');
const request = require('request');
app.use(bodyParser.urlencoded({ limit: '50mb',extended: true }));
const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('mfc.sqlite');
db.run('PRAGMA busy_timeout = 6000');
db.configure("busyTimeout", 6000);

const mfcControllers = ["mfc1","mfc2","mfc3","mfc4"];

let csData = {};

function refactorCS(data){
  let json = {};
  json['envionment'] = data.head.envionment;
  for (var i = 0; i < data.head.fields.length; i++) {
    json[data.head.fields[i].name] = {
      "units": data.head.fields[i].units,
      "data": data.data[0].vals[i]
    };
  }
  return json;
}

function getCS(){
  let url = "http://10.12.16.10/?command=dataquery&uri=dl:Public&format=json&mode=most-recent";
  request(url, function (error, response, body) {
    if (!error && response.statusCode == 200) {
      csData = refactorCS(JSON.parse(body));
      //console.log("getCS",csData);
    }
    else {
      console.log("Request Error "+response.statusCode)
      csData = {"mfc":req.params.mfc, "action":"get cr6 public","data":response.statusCode};
    }
  });
}
getCS()
setInterval(function () { getCS(); }, 30 * 1000);

// Server port
const HTTP_PORT = 8000
// Start server
app.listen(HTTP_PORT, () => {
    console.log(moment().format('YYYY-MM-DD HH:mm:ss'), "- Server running on port %PORT%".replace("%PORT%",HTTP_PORT),Date.now())
});

app.use(function (req, res, next) {
    console.log(moment().format('YYYY-MM-DD HH:mm:ss'),"- The file " + req.url + " was requested.");
    next();
});

// Serve files in public folder
app.use('/', express.static('public'));
app.use('/logs/', serveIndex('logs'));
// Root endpoint
app.get("/api", (req, res, next) => {
  let routes = "";
  app._router.stack.forEach(function(r){
    if (r.route && r.route.path){
      routes += r.route.path + ": " + JSON.stringify(r.route.methods) + "<br />";
    }
  })
  //res.send(routes);
  res.send("v0.1");
});

app.get("/api/status", (req, res, next) => { // get the controller status
  let sql = "SELECT * FROM mfcstatus";
  //console.log(sql);
  db.all(sql, function(err, rows) {
    res.json({"mfc":req.params.mfc, "action":"status","data":rows});
  });
});

app.get("/api/cr6/public", (req, res, next) => { // get the controller status
  res.json(csData);
});


app.post("/api/:mfc/status", (req, res, next) => { // toggle the controller status
  if(mfcControllers.includes(req.params.mfc)) {
    let sql = "UPDATE mfcstatus SET status = ((status | 1) - (status & 1)) WHERE id = " + req.params.mfc.charAt(3);
    db.run(sql, function(err, rows) {
      db.get("SELECT * FROM mfcstatus WHERE id = " + req.params.mfc.charAt(3), function(err, row) {
        res.json(row);
      });
    });
  }
});

app.get("/api/mfcnames", (req, res, next) => { // rename the controller
  let mfcnamefile = JSON.parse(fs.readFileSync('mfc-names.json'));
  res.json(mfcnamefile);
});

app.post("/api/:mfc/namemfc", (req, res, next) => { // rename the controller
  let mfcNo = req.params.mfc;
  let receivedData = JSON.parse(req.body.data);
  if(mfcControllers.includes(req.params.mfc)) {
    res.json({"mfcNo":mfcNo,"receivedData":receivedData});
    let mfcnamefile = JSON.parse(fs.readFileSync('mfc-names.json'));
    console.log(mfcnamefile);
    mfcnamefile[mfcNo] = receivedData;
    console.log(mfcnamefile);
    fs.writeFileSync('mfc-names.json', JSON.stringify(mfcnamefile));
  }
});

app.get("/api/:mfc/nextschedule", (req, res, next) => { // get the next sechedule entry
  const shutdownVariables = {
    'mfc1':["MFC1CO2Shutdown","MFC1AirShutdown"],
    'mfc2':["MFC2CO2Shutdown","MFC2AirShutdown"],
    'mfc3':["MFC3CO2Shutdown","MFC3AirShutdown"],
    'mfc4':["MFC4CO2Shutdown","MFC4AirShutdown"]
  }
  if(typeof(csData[shutdownVariables[req.params.mfc][0]].data) !== 'undefined' && typeof(csData[shutdownVariables[req.params.mfc][1]].data) !== 'undefined') {
    if(csData[shutdownVariables[req.params.mfc][0]].data == 0 && csData[shutdownVariables[req.params.mfc][1]].data == 0) {
      if(mfcControllers.includes(req.params.mfc)) {
        //let sql = "SELECT * FROM " + req.params.mfc + " WHERE co2complete = 0 AND aircomplete = 0 ORDER BY abs(changetime - (strftime('%s', 'now')*1000)) LIMIT 1"; // TODO look at this for a better query. Possibly add where less than now
        let sql = "SELECT * FROM " + req.params.mfc + " WHERE co2complete = 0 AND aircomplete = 0 AND (SELECT s.status FROM mfcstatus s WHERE s.controller = '" + req.params.mfc + "') = 1 AND (strftime('%s', 'now')*1000) - changetime < 86400000 ORDER BY abs(changetime - (strftime('%s', 'now')*1000)) LIMIT 1;"
        db.all(sql, function(err, rows) {
          res.json({"mfc":req.params.mfc, "action":"get schedule","data":rows});
        });
      } else {
        res.json({"mfc":req.params.mfc, "action":"get schedule","data":"ERROR"});
      }
    } else {
      res.json({"mfc":req.params.mfc, "action":"get schedule","data":[{ id: 0, changetime: moment.unix(Date.now()/1000)-500, co2value: 0, co2complete: 0, co2complete_time: 0, airvalue: 0, aircomplete: 0, aircomplete_time: 0 }]});
    }
  } else {
    res.json({"mfc":req.params.mfc, "action":"get schedule","data":"ERROR"});
  }
});

app.get("/api/:mfc/schedule", (req, res, next) => { // get the entire sechedule
  if(mfcControllers.includes(req.params.mfc)) {
//    let sql = "SELECT * FROM " + req.params.mfc + " WHERE changetime >= " + moment.unix(Date.now()/1000).subtract(2, 'hours') + " ORDER BY changetime";
    let sql = "SELECT * FROM " + req.params.mfc + " ORDER BY changetime";
    //console.log(sql);
    db.all(sql, function(err, rows) {
      res.json({"mfc":req.params.mfc, "action":"get schedule","data":rows});
    });
  } else {
    res.json({"mfc":req.params.mfc, "action":"get schedule","data":"ERROR"});
  }
});

app.post("/api/:mfc/addschedule", (req, res, next) => { // post the entire schedule
    let mfcNo = req.params.mfc;
    //console.log(JSON.parse(req.body.data));
    let receivedData = JSON.parse(req.body.data);
    res.json({"mfc":req.params.mfc, "action":"add to schedule", "got": receivedData.length});
    for (var i = 0; i < receivedData.length; i++) {
      if(typeof(receivedData[i].co2) !== "undefined" && typeof(receivedData[i].air) !== "undefined") {
        receivedData[i]
        let sql = "INSERT INTO " + mfcNo + " (changetime,co2value,airvalue) VALUES("+receivedData[i].time+","+receivedData[i].co2+","+receivedData[i].air+");";
        //console.log(sql);
        db.run(sql, function(err, rows) {

        });
      }
    }
});

app.post("/api/:mfc/markdone", (req, res, next) => { // post mark value changed time
    let mfcNo = req.params.mfc;
    console.log(req.body);
    let receivedData = req.body;
    // { dbKey: '1254', mfcType: 'air' }
    res.json({"mfc":req.params.mfc, "action":"mark done", "got": receivedData});
    if(typeof(receivedData.dbKey) !== "undefined" && typeof(receivedData.mfcType) !== "undefined") {
      let sql = "UPDATE " + mfcNo + " SET " + receivedData.mfcType + "complete = 1, " + receivedData.mfcType + "complete_time = " + moment.unix(Date.now()/1000) + " WHERE id = " + receivedData.dbKey + ";";
      //console.log(sql);
      db.run(sql, function(err, rows) {

      });
    }

});

app.post("/api/:mfc/deletevalue/:id", (req, res, next) => { // toggle the controller status
  if(mfcControllers.includes(req.params.mfc)) {
    let mfcNo = req.params.mfc;
    let idNo = req.params.id;
    let sql = "DELETE FROM " + mfcNo + " WHERE id = " + idNo;
    //console.log(sql);
    db.run(sql, function(err, rows) {
      console.log({"mfc":mfcNo, "action":"get schedule","data":mfcNo+idNo});
      res.json({"mfc":mfcNo, "action":"get schedule","data":mfcNo+idNo})
    });
  }
});

app.post("/api/:mfc/deleteall", (req, res, next) => { // toggle the controller status
  if(mfcControllers.includes(req.params.mfc)) {
    let mfcNo = req.params.mfc;
    let sql = "DELETE FROM " + mfcNo;
    //console.log(sql);
    db.run(sql, function(err, rows) {
      console.log({"mfc":mfcNo, "action":"deleteall","data":mfcNo});
      res.json({"mfc":mfcNo, "action":"deleteall","data":mfcNo})
    });
  }
});

app.post("/api/:mfc/resetshutdown", (req, res, next) => { // reset shutdown state in the datalogger
  if(mfcControllers.includes(req.params.mfc)) {
    let mfcNo = req.params.mfc;
    let auth = "Basic " + new Buffer("hakai:Kwakshua").toString("base64");
    let url = "http://10.12.16.10/?command=setvalueex&uri=dl:Public." + mfcNo.toUpperCase() + "CO2Shutdown&value=0&format=json";
    let headers = { headers : { "Authorization" : auth }};
    request(url, headers, function (error, response, body) {
      if (!error && response.statusCode == 200) {
        res.json({"mfc":req.params.mfc, "action":"reset shutdown", "got": JSON.parse(body)});
        console.log({"mfc":req.params.mfc, "action":"reset shutdown", "got": JSON.parse(body)});
      } else {
        res.json({"mfc":req.params.mfc, "action":"reset shutdown", "got": response.statusCode});
        console.log({"mfc":req.params.mfc, "action":"reset shutdown", "got": response.statusCode});
      }
    });
    let url2 = "http://10.12.16.10/?command=setvalueex&uri=dl:Public." + mfcNo.toUpperCase() + "AirShutdown&value=0&format=json";
    request(url2, headers, function (error, response, body) {
      if (!error && response.statusCode == 200) {
        //res.json({"mfc":req.params.mfc, "action":"reset shutdown", "got": JSON.parse(body)});
        console.log({"mfc":req.params.mfc, "action":"reset shutdown", "got": JSON.parse(body)});
      } else {
        //res.json({"mfc":req.params.mfc, "action":"reset shutdown", "got": response.statusCode});
        console.log({"mfc":req.params.mfc, "action":"reset shutdown", "got": response.statusCode});
      }
    });
  }
});

// app.post("/api/:mfc/current/:valType", (req, res, next) => { // post value to the controller
//     let mfcNo = req.params.mfc;
//     let valType = req.params.valType;
//
//
//     res.json({"mfc":req.params.mfc, "action":"post current " + valType + " value"});
// });

// CR6 Username and Password hakai:Kwakshua

// Default response for any other request
app.use(function(req, res){
    res.status(404);
});