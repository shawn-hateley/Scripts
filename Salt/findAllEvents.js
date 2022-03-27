//Read the US processedUS file and find all events from the DS data. Create individual event files. Requires an up to date US processedUS file.
const lineByLine = require('n-readlines'); //This is the library to read thr file line by line
const fs = require('fs'); //file system for appending the files
var glob = require("glob");

//var rawDirectoryName = "/home/shawn/saltDose/" //Raw data directory on PC
var rawDirectoryName = "/Users/shawnhateley/Projects/Test_Data/saltDose/" //Raw data directory on macBook
//var rawDirectoryName = "/data/www/saltDose/" //Raw data directory on hecate
var stationDirectory = rawDirectoryName + "CollatedData/Stations/"; //Read from the collated data / Stations directories. Get names from dir.
var stationList = fs.readdirSync(stationDirectory); //Get the folder names of the stations. SSN626, SSN703, etc
console.log(stationList);

//Get the list of files that match the station
for (var k = 0; k < stationList.length; k++) {
  if (stationList[k].includes(".DS")) continue //skip mac .DS files
  var stationName = stationList[k]; //use this as a simpler variable

  var fileList = [];
  fileList = glob.sync(rawDirectoryName + "/**/" + stationName + "DS_DoseEvent.*") //get a list of all of the ds dose event files from all directories
  console.log(fileList);

  var processedUSFile = stationDirectory + stationName + "/" + stationName + "US_DoseEvent.dat.csv" //This is the master list of events
  if (stationName == "SSN626"){
     processedUSFile = stationDirectory + stationName + "/" + stationName + "AS_DoseEvent.dat.csv";
   }
  console.log(processedUSFile);

  //var rawUSFileName = rawDirectoryName + processedUSFile.split("/").pop(); //take the processedUSFile and combine it with the raw directory.
  //var rawDSFileName = rawDirectoryName + stationName + "DS_DoseEvent.dat.csv";

  var processedUS = new lineByLine(processedUSFile); //creates new object to read individual lines
  //var raw = new lineByLine(rawUSFileName);
  //var rawDS = new lineByLine(rawDSFileName);
  //var processedHeader = readHeader(processedUS); //Get the master file header for comparison. Only the second line of the file is returned
  console.log("calling matchline function");

  matchLine(processedUS); //Find the line that matches the last line from the master

 }



function matchLine(matchFile){
  var line;
  var lineWrite = 0;
  var lineText;
  var eventID;
  var eventLine;
  var eventText;
  var lineNumber = 0

  while (line = matchFile.next()){ //iterate over all lines in the US doseevent file
    lineText = line.toString('ascii').replace(/["]+/g, ''); //convert the line object to a string and strip the double quotes

    if (lineNumber < 4) {
      lineNumber++;
      continue; //skip header
    }

    eventID = lineText.split(",")[2]; //get event id so it can be used to create individual ds event files
    console.log(eventID)
    if (isNaN(eventID)) continue; //Check to see if the EventID is a lineNumber

    fs.writeFileSync(stationDirectory + stationName + "/" + eventID + ".csv",""); //create event file. Deletes old versions if they exist
    console.log("creating file  " + stationDirectory + stationName + "/" + eventID + ".csv");


    for (var j = 0; j < fileList.length; j++){
      var raw = new lineByLine(fileList[j]);

      matchEvent(eventID,raw); //call function to match and copy DS event data

    }
  }
}

function matchEvent(eventID,rawDS){
  var eventLine;
  var eventText;
  var reset = 0;
  var lineNumber = 0;
  var header = [];
  var headerReset = 0;

  //console.log("Starting matchEvent Function")

  while (eventLine = rawDS.next()){ //iterate over all the lines in the DS doseevent file
    eventText = eventLine.toString('ascii'); //convert the line object to a string
    //console.log(eventID);
    if (lineNumber < 4){ //Create header for each new file using the current file
      //fs.appendFileSync(stationDirectory + stationName + "/" + eventID + ".csv", eventText + '\n');
      header[lineNumber] = (eventText + '\n')
      lineNumber++;
      continue
    }

    if (eventText.split(",")[2].replace(/["]+/g, '') == eventID){
      if (headerReset == 0){
        for (var i = 0; i < 4; i++){
          fs.appendFileSync(stationDirectory + stationName + "/" + eventID + ".csv", header[i]);
        }
        headerReset = 1;
      }
      console.log("matched DS Event ID");
      fs.appendFileSync(stationDirectory + stationName + "/" + eventID + ".csv", eventText + '\n');
      console.log(eventText + '\n');
      reset = 1;
    }else if (reset == 1){
      eventLine = rawDS.reset();
      console.log("Reseting File");
      break;
    // }else{
      //console.log("Skipping DS");
    }
  }
}
