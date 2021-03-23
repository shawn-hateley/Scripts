//Read the header of the master file to compare with the header of the raw file. If it matches, then find the last entry in the master file
//to compare with the raw file. Append all remaining lines from the raw file to the master.
const lineByLine = require('n-readlines'); //This is the library to read thr file line by line

var glob = require("glob") //search directory for files
const fs = require('fs'); //file system for appending the files

let directory_name = "/Users/shawnhateley/Projects/Test_Data/saltDose/CollatedData/Stations/"; //Read from the collated data / Stations directories. Get names from dir.
let rawDirectoryName = "/Users/shawnhateley/Projects/Test_Data/saltDose/" //Raw data directory
let masterList = fs.readdirSync(directory_name); //Get the folder names of the stations. SSN626, SSN703, etc
console.log(masterList);

//Get the list of files that match the station
for (var k = 0; k < masterList.length; k++) {
  if (masterList[k].includes(".DS")) continue //skip mac .DS files
  var stationName = masterList[k];
  var masterFileName = glob.sync(directory_name + masterList[k] + "/" + "*_DoseEvent.dat.csv")[0]; //find the name of the upstream doseevent file SSN626AS_DoseEvent.dat.csv
  var rawFileName = rawDirectoryName + masterFileName.split("/").pop(); //take the masterfilename and combine it with the raw directory.
  var rawDSFileName = rawDirectoryName + masterList[k] + "DS_DoseEvent.dat.csv";
  //console.log(masterFileName);
  //console.log(rawFileName);
  console.log(rawDSFileName)
  var master = new lineByLine(masterFileName); //creates new object to read individual lines
  var raw = new lineByLine(rawFileName);
  var rawDS = new lineByLine(rawDSFileName);
  var masterHeader = readHeader(master); //Get the master file header for comparison. Only the second line of the file is returned
  var rawHeader = readHeader(raw);

  if (masterHeader == rawHeader.replace(/["]+/g, '')){ //strip away double quotes from the data before comparing
    var lastMasterLine = lastLine(master); //Get the last line of the masterList
    //console.log(lastMasterLine);
    console.log("header match")
    matchLine(raw); //Find the line that matches the last line from the master
  }else {
    console.log("No header Match");
  }

}



function readHeader(fileName){ //function to read the second line (header) from the file passed to it. Data file headers changed in 2016
  var line;
  var lineNumber = 0;

  while (lineNumber < 2) { //do this until the second line is reached
      line = fileName.next()
      if (lineNumber == 1){
        return line.toString('ascii'); //convert the line to a string
      }
      lineNumber++;
  }
}

function lastLine(fileName){ //Function to return the last line of a file. This is needed so we only add new data to the end of the file
  var line;
  var lineNumber = 0;
  var lineText;

  while (line = fileName.next()){
    lineNumber++;
    lineText = line; //used so the last line is remembered and can be returned before the function ends
  }
  return lineText.toString('ascii');
}

function matchLine(matchFile){
  var line;
  var lineWrite = 0;
  var lineText;
  var eventID;
  var eventLine;
  var eventText;
  console.log(lastMasterLine);

  while (line = matchFile.next()){ //iterate over all lines in the US doseevent file
      lineText = line.toString('ascii').replace(/["]+/g, ''); //convert the line object to a string and strip the double quotes

      if (lineWrite == 1){ //append the current line to the master file
        fs.appendFileSync(masterFileName,'\n') //make sure the newest line starts below the last one
        fs.appendFileSync(masterFileName,lineText + '\n')
        console.log("Appending");
        console.log(masterFileName + lineText + '\n')
        eventID = lineText.split(",")[2]; //get event id so it can be used to create individual ds event files
        console.log(eventID)
        fs.writeFileSync(directory_name + stationName + "/" + eventID + ".csv","");
        console.log(directory_name + stationName + "/" + eventID + ".csv");

        matchEvent(eventID);
        // while (eventLine = rawDS.next()){ //iterate over all the lines in the DS doseevent file
        //   eventText = eventLine.toString('ascii').replace(/["]+/g, ''); //convert the line object to a string and strip the double quotes
        //   if (eventText.split(",")[2] == eventID){
        //     console.log("matched DS Event ID");
        //     //fs.appendFileSync(directory_name + stationName + "/" + eventID + ".csv", eventText + '\n');
        //     console.log(eventText + '\n');
        //   }else{
        //     console.log("Skipping DS");
        //   }
        // }



      }else{
        console.log("Skipping");
      }

      if (lineText.split(",")[2] == lastMasterLine.split(",")[2]){ //check if the current line matches the last line of the master file by comparing event ids
        console.log("Match Found, appending next line unless end of file");
        console.log(lineText);
        lineWrite = 1; //Set to 1 so the file appending can begin with the next line
      }

  }
}

function matchEvent(eventID){
  var eventLine;
  var eventText;
  var reset = 0;

  console.log("Starting matchEvent Function")

  while (eventLine = rawDS.next()){ //iterate over all the lines in the DS doseevent file
    eventText = eventLine.toString('ascii').replace(/["]+/g, ''); //convert the line object to a string and strip the double quotes
    //console.log(eventID);

    if (eventText.split(",")[2] == eventID){
      console.log("matched DS Event ID");
      fs.appendFileSync(directory_name + stationName + "/" + eventID + ".csv", eventText + '\n');
      console.log(eventText + '\n');
      reset = 1;
    }else if (reset == 1){
      eventLine = rawDS.reset();
      console.log("Reseting File");
      break;
    }else{
      console.log("Skipping DS");
    }
  }
}
