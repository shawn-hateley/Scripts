//Read the header of the processedUS file to compare with the header of the rawUS file. If it matches, then find the last entry in the processedUS file
//to compare with the rawUS file. Append all remaining lines from the rawUS file to the processedUS file.

//This script needs to check all directories and deal with file changes. Archiving the data will create new files and the check for previous lines will fail.
//I could check for previous files and if that is last line in a file, then copy all newer lines. (check the dates)


const lineByLine = require('n-readlines'); //This is the library to read thr file line by line
const fs = require('fs'); //file system for appending the files

//var rawDirectoryName = "/home/shawn/saltDose/" //rawUS data directory on PC
var rawDirectoryName = "/Users/shawnhateley/Projects/Test_Data/saltDose/" //rawUS data directory on macBook
//var rawDirectoryName = "/data/www/saltDose/" //rawUS data directory on hecate
var stationDirectory = rawDirectoryName + "CollatedData/Stations/"; //Read from the collated data / Stations directories. Get names from dir.
var stationList = fs.readdirSync(stationDirectory); //Get the folder names of the stations. SSN626, SSN703, etc
console.log(stationList);

//Get the list of files that match the station
for (var k = 0; k < stationList.length; k++) {
  if (stationList[k].includes(".DS")) continue //skip mac .DS files
  var stationName = stationList[k]; //use this as a simpler variable (eg. SSN703)
  var processedUSFile = stationDirectory + stationName + "/" + stationName + "US_DoseEvent.dat.csv" // "/home/shawn/saltDose/CollatedData/Stations/SSN703/SSN703US_DoseEvent.dat.csv"
  if (stationName == "SSN626") processedUSFile = stationDirectory + stationName + "/" + stationName + "AS_DoseEvent.dat.csv";

  var rawUSFileName = rawDirectoryName + processedUSFile.split("/").pop(); //take the processedUSFile and combine it with the rawUS directory.  "/home/shawn/saltDose/SSN703US_DoseEvent.dat.csv"
  var rawDSFileName = rawDirectoryName + stationName + "DS_DoseEvent.dat.csv"; //"/home/shawn/saltDose/SSN703DS_DoseEvent.dat.csv"

  var processedUS = new lineByLine(processedUSFile); //creates new object to read individual lines
  var rawUS = new lineByLine(rawUSFileName);
  var rawDS = new lineByLine(rawDSFileName);
  var processedHeader = readHeader(processedUS); //Get the master file header for comparison. Only the second line of the file is returned
  var rawHeader = readHeader(rawUS);

  if (processedHeader == rawHeader.replace(/["]+/g, '')){ //strip away double quotes from the data before comparing. Make sure headers are the same before adding new rows
    var lastProcessedLine = lastLine(processedUS); //Get the last line of the processedUS US_DoseEvent file

    console.log("header match")
	  console.log("Opening File " + rawUSFileName);
    //console.log("Last Processed Line " + lastProcessedLine);
    matchLine(rawUS); //Find the line that matches the last line from the processedUS US_DoseEvent file
  }else {
    console.log("No header Match");
	//console.log(processedHeader)
	//console.log(rawHeader.replace(/["]+/g, ''))
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

function matchLine(matchFile){ //matchFile is the rawUS US DoseEvent file
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
        console.log(processedUSFile + " " + lineText + '\n')

        eventID = lineText.split(",")[2]; //get event id so it can be used to create individual ds event files
        if (isNaN(eventID)) continue; //Check to see if the EventID is a lineNumber
        console.log(eventID)

      }else{
        console.log("Skipping");
      }

      if (lineText.split(",")[2] == lastProcessedLine.split(",")[2]){ //check if the current line matches the last line of the master file by comparing event ids
        console.log("Match Found, appending next line unless end of file");
        console.log(lineText);
        lineWrite = 1; //Set to 1 so the file appending can begin with the next line
      }else{
		console.log("No Match");
		//console.log("rawUS " + lineText.split(",")[2]);
		//console.log("processedUS " + lastProcessedLine.split(",")[2]);
	  }

  }
}
