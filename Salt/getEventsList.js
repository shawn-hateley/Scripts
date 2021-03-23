//Read the header of the master file
const lineByLine = require('n-readlines');
const masterDirectory = "/Users/shawnhateley/Projects/MasterFileList"; //collated UpStream data files as produced by readline.js
const searchDirectory = "/Users/shawnhateley/Projects/LatestData" //collated DownStream data files as produced by readline.js
const fs = require('fs');

let masterList = fs.readdirSync(masterDirectory); //get the list of all the US files
console.log(masterList);

//Take the list of files from the masterDirectory and get the list of Event ID's from each
for (var k = 0; k < masterList.length; k++) {
  var masterFileName = masterDirectory + "/" + masterList[k];
  var master = new lineByLine(masterFileName);
  var masterEventList= getEventID(master); //call the getEventID function

  fs.writeFileSync(masterFileName + ".event.csv",masterEventList); //Write the output of the getEventID function
}



function getEventID(fileName){ //function to read the EventID from each of the US Dose Event files
  let line;
  var outputArray = []

  while (line = fileName.next()) {
      //line = fileName.next()
      if (isNaN(line.toString('ascii').split(",")[2])){ //check if the line is the header or blank
        continue
      } else {
        outputArray.push(line.toString('ascii').split(",")[2]); //create array of the Event Ids
      }
  }
  return outputArray;
}
