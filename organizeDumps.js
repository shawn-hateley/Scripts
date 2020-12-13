//Read the header of the master file
const lineByLine = require('n-readlines');
const masterDirectory = "/Users/shawnhateley/Projects/MasterFileList"; //US data files
const searchDirectory = "/Users/shawnhateley/Projects/LatestData" //DS data files
var glob = require("glob") //search directory for files
const fs = require('fs');
//var SSN626,SSN703,SSN844,SSN1015;
//var stations = [626,703,844,1015];

let masterList = fs.readdirSync(masterDirectory); //get the list of all the US files
console.log(masterList);

//Take the list of files from the masterDirectory and get the list of Event ID's from each
for (var k = 0; k < masterList.length; k++) {
  var masterFileName = masterDirectory + "/" + masterList[k];
  var master = new lineByLine(masterFileName);
  var masterEventList= getEventID(master); //Get the master file header for comparison

  //console.log(masterEventList)
  fs.writeFileSync(masterFileName + ".event.csv",masterEventList);
  //call function to get DS events and write them to files
  //getDSEvents(masterEventList)
}



function getEventID(fileName){ //function to read the EventID from each of the US Dose Event files
  let line;
  var outputArray = []

  while (line = fileName.next()) {
      //line = fileName.next()
      if (isNaN(line.toString('ascii').split(",")[2])){
        continue
      } else {
        outputArray.push(line.toString('ascii').split(",")[2]);
      }
  }
  return outputArray;
}
