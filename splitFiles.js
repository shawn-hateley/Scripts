const lineByLine = require('n-readlines');
const searchDirectory = "/Users/shawnhateley/Projects/LatestData" //DS data files
var glob = require("glob") //search directory for files
const fs = require('fs');


let eventFileList = fs.readdirSync(searchDirectory);
console.log(eventFileList);
//console.log(eventFileList.length)

for (var j = 0; j < eventFileList.length; j++) {
  if (eventFileList[j].includes(".DS")) continue
  var dsFileName = searchDirectory + "/" + eventFileList[j];
  var dsFile = new lineByLine(dsFileName);
  console.log(dsFileName)
  writeDSEvents(dsFile);
}



function writeDSEvents(fileName){
  let line;
  var currentID;
  var nextID;
  var header = [];
  var lineNumber = 0;

  while (line = fileName.next()) {
    if (lineNumber < 4){
      header.push(line.toString('ascii'))
      lineNumber++;
      continue
    }

    if (isNaN(line.toString('ascii').split(",")[2])){
      continue
    } else {
      nextID = line.toString('ascii').split(",")[2];
      if (nextID == currentID){
        fs.appendFileSync(searchDirectory + "/" + currentID + ".csv", line.toString('ascii'))
      } else {
        fs.writeFileSync(searchDirectory + "/" + nextID + ".csv", header)
        fs.appendFileSync(searchDirectory + "/" + nextID + ".csv", line.toString('ascii'))
      }
      currentID = nextID;
    }
}
}
