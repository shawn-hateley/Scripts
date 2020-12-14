const lineByLine = require('n-readlines');
const searchDirectory = "/Users/shawnhateley/Projects/Events" //collated DownStream data files as produced by readline.js
const outputDirectory = "/Users/shawnhateley/Projects/LatestData/"
const fs = require('fs');


let eventFileList = fs.readdirSync(searchDirectory); //Get the name of the files in the directory
//console.log(eventFileList);
console.log(eventFileList.length)


for (var j = 0; j < eventFileList.length; j++) {
  if (eventFileList[j].includes(".DS")) continue //skip mac .DS files
  var dsFileName = searchDirectory + "/" + eventFileList[j];
  var dsFile = new lineByLine(dsFileName);
  console.log(dsFileName)

  var outputFolder = dsFile.next().toString('ascii').split(",")[1].slice(1, -1) + "/" + eventFileList[j];
  console.log(outputFolder)
  var outputFileName = outputDirectory + outputFolder;
  console.log(outputFileName)

  fs.renameSync(dsFileName,outputFileName)
}
