//Read the header of the processed file to compare with the header of the raw file. If it matches, then find the last entry in the processed file
//to compare with the raw file. Append all remaining lines from the raw file to the processed file.
const lineByLine = require('n-readlines'); //This is the library to read thr file line by line
const fs = require('fs'); //file system for appending the files

var rawDirectoryName = "/home/shawn/saltDose/" //Raw data directory on PC
//var rawDirectoryName = "/Users/shawnhateley/Projects/Test_Data/saltDose/" //Raw data directory on macBook
//var rawDirectoryName = "/data/www/saltDose/" //Raw data directory on hecate
var stationDirectory = rawDirectoryName + "CollatedData/Stations/"; //Read from the collated data / Stations directories. Get names from dir.
var stationList = fs.readdirSync(stationDirectory); //Get the folder names of the stations. SSN626, SSN703, etc
console.log(stationList);

//Get the list of files that match the station
for (var k = 0; k < stationList.length; k++) {
  if (stationList[k].includes(".DS")) continue //skip mac .DS files
  var stationName = stationList[k]; //use this as a simpler variable (eg. SSN703)
  var processedUSFile = stationDirectory + stationName + "/" + stationName + "US_DoseEvent.dat.csv" // "/home/shawn/saltDose/CollatedData/Stations/SSN703/SSN703US_DoseEvent.dat.csv"
  if (stationName == "SSN626") processedUSFile = stationDirectory + stationName + "/" + stationName + "AS_DoseEvent.dat.csv";

  var rawUSFileName = rawDirectoryName + processedUSFile.split("/").pop(); //take the processedUSFile and combine it with the raw directory.  "/home/shawn/saltDose/SSN703US_DoseEvent.dat.csv"
  var rawDSFileName = rawDirectoryName + stationName + "DS_DoseEvent.dat.csv"; //"/home/shawn/saltDose/SSN703DS_DoseEvent.dat.csv"

  var processed = new lineByLine(processedUSFile); //creates new object to read individual lines
  var raw = new lineByLine(rawUSFileName);
  var rawDS = new lineByLine(rawDSFileName);
  var processedHeader = readHeader(processed); //Get the master file header for comparison. Only the second line of the file is returned
  var rawHeader = readHeader(raw);

  if (processedHeader == rawHeader.replace(/["]+/g, '')){ //strip away double quotes from the data before comparing. Make sure headers are the same before adding new rows
    var lastProcessedLine = lastLine(processed); //Get the last line of the processed US_DoseEvent file

    console.log("header match")
	console.log("Opening File " + rawUSFileName);
    matchLine(raw); //Find the line that matches the last line from the processed US_DoseEvent file
  }else {
    console.log("No header Match");
	console.log(processedHeader)
	console.log(rawHeader.replace(/["]+/g, ''))
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

function matchLine(matchFile){ //matchFile is the raw US DoseEvent file
  var line;
  var lineWrite = 0;
  var lineText;
  var eventID;
  var eventLine;
  var eventText;

  while (line = matchFile.next()){ //iterate over all lines in the US doseEvent file
      lineText = line.toString('ascii').replace(/["]+/g, ''); //convert the line object to a string and strip the double quotes
	  

      if (lineWrite == 1){ //append the current line to the master file
        //fs.appendFileSync(processedUSFile,'\n') //make sure the newest line starts below the last one
        fs.appendFileSync(processedUSFile,'\n' + lineText)
        console.log("Appending");
        console.log(processedUSFile + lineText + '\n')

        eventID = lineText.split(",")[2]; //get event id so it can be used to create individual ds event files
        console.log(eventID)
/*
        fs.writeFileSync(stationDirectory + stationName + "/" + eventID + ".csv",""); //create event file. Deletes old versions if they exist
        console.log("creating file  " + stationDirectory + stationName + "/" + eventID + ".csv");

        matchEvent(eventID); //call function to match and copy DS event data
*/
      }else{
        console.log("Skipping");
      }

      if (lineText.split(",")[2] == lastProcessedLine.split(",")[2]){ //check if the current line matches the last line of the master file by comparing event ids
        console.log("Match Found, appending next line unless end of file");
        console.log(lineText);
        lineWrite = 1; //Set to 1 so the file appending can begin with the next line
      }else{
		console.log("No Match");
		console.log("Raw " + lineText.split(",")[2]);
		console.log("Processed " + lastProcessedLine.split(",")[2]);
	  }

  }
}
/*
function matchEvent(eventID){
  var eventLine;
  var eventText;
  var reset = 0;
  var lineNumber = 0;

  //console.log("Starting matchEvent Function")

  while (eventLine = rawDS.next()){ //iterate over all the lines in the DS doseevent file
    eventText = eventLine.toString('ascii').replace(/["]+/g, ''); //convert the line object to a string and strip the double quotes
    //console.log(eventID);
    if (lineNumber < 4){ //Create header for each new file using the current file
      fs.appendFileSync(stationDirectory + stationName + "/" + eventID + ".csv", eventText + '\n');
      //header.push(line.toString('ascii'))
      lineNumber++;
      continue
    }

    if (eventText.split(",")[2] == eventID){
      console.log("matched DS Event ID");
      fs.appendFileSync(stationDirectory + stationName + "/" + eventID + ".csv", eventText + '\n');
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
*/
