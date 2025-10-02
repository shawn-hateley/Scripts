//Open lastProcessed file 
//Read the date from the file. This is the date and time of the last record that was processed

//Open onehour.dat file
//Read the date of the last measurement and compare to the lastProcessed file. If younger, then proceed.
//Read the header to find the location of each measurement
//Match each measurement needed and put in variable
//Create datatable that matches existing Wunderground structure
//Write to file
//Update lastProcessed file. 


const fs = require('fs');
const { parse } = require("csv-parse");
var moment = require('moment');

//File names and paths
var lastProcessedFile = "/Users/shawnhateley/Projects/Test_Data/lastProcessed"
var oneHourFile = "/Users/shawnhateley/Projects/Test_Data/OrfordBuoy_FiveMin.dat"
var outputFileName = "OrfordBuoy_Wunderground_"
var outputPath = "/Users/shawnhateley/Projects/Test_Data/"
var outputFile;

var lastProcessedDate;
var rowDate;
var map = [];

//header info from/for the fivemin table and the wunderground file
const headers = ["TIMESTAMP","RECORD","WindDir_Avg","WindDir_Std","WindSpd_Avg","WindSpd_Max","AirTemp_Avg","StationAirPressure_Avg"]
const wundergroundTOA = ["\"TOACI1\"","\"OrfordBuoy\"","\"Wunderground\""]
const wundergroundHeaders = ["\"TMSTAMP\"","\"RECNBR\"","\"winddir\"","\"winddir_STDEV\"","\"windspeedmph\"","\"windguestmph\"","\"tempf\"","\"baromin\""]
var lastProcessedRaw 

//Read in the date and time of the last processes file. Check if file exists. If not assign arbitrary date that should be old enough
if (fs.existsSync(lastProcessedFile)) {
    lastProcessedRaw = fs.readFileSync(lastProcessedFile, { encoding: 'utf8', flag: 'r' });
  } else {
    lastProcessedRaw = "Wed Oct 01 2025 15:00:00 GMT-0700 (Pacific Daylight Saving Time)"
  }


lastProcessedDate = new Date(lastProcessedRaw) //convert string to date

console.log("Last Processed Date " + lastProcessedDate);

fs.createReadStream(oneHourFile)  //read in the data from the FiveMin dat file, one row at a time
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    //match headers with output of row. if match, then get position. 
    for (var i = 0; i < headers.length; i++){
        //search row for matches with header using indexOf
        if (row.indexOf(headers[i]) > -1) {
            map.push(row.indexOf(headers[i]))
        }
    }

    rowDate = new Date(row[0]); //get the date and time from the row of data being processed

    if (rowDate.getTime() > lastProcessedDate.getTime()){ //check to see if the row date matches the last processed date.
        var output = [];
        var measurements = [];
        console.log("new data found")
        for (var i = 0; i < headers.length; i++){

            if (headers[i] == "WindSpd_Avg"){ //Convert windspeed from m/s to mph
                measurements.push((row[map[i]] * 2.23694).toFixed(3))
            } else if (headers[i] == "WindSpd_Max"){
                measurements.push((row[map[i]] * 2.23694).toFixed(3))
            } else if (headers[i] == "AirTemp_Avg"){ //convert C to F
                measurements.push((row[map[i]] * 1.8 + 32).toFixed(3))
            } else {
                measurements.push(row[map[i]]); //get the measurements that match the positions of the headers
            }
        } 

        measurements[0] = "\"" + measurements[0] + "\""; //add "" to the timestamp
        outputFile = outputPath + outputFileName + moment(rowDate).format("YYYY_MM_DD_HHmm") + ".dat"
        output.push(wundergroundTOA,wundergroundHeaders,measurements)

        fs.writeFile(outputFile, output.join('\n'), 'utf8', (err) => {
            if (err) {
              console.error('Error writing file:', err);
              return;
            }
            console.log('Wunderground file written successfully!');
          });
        
    }
})

.on("end", function () {
  console.log("finished");

  fs.writeFile(lastProcessedFile, rowDate.toString(), 'utf8', (err) => {
    if (err) {
      console.error('Error writing file:', err);
      return;
    }
    console.log('LastProcessedFile updated');
  });

})

.on("error", function (error) {
  console.log(error.message);
});




