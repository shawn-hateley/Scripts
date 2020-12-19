//Read the header of the master file
const lineByLine = require('n-readlines');
//const masterFileName = "/Users/shawnhateley/Projects//SSN703US_DoseEvent.dat.master.csv"
//const master = new lineByLine(masterFileName);
//var masterHeader = readHeader(master); //Get the master file header for comparison

var glob = require("glob") //search directory for files
const fs = require('fs');

//var stations = ["SSN703","SSN844","SSN626","SSN1015"]
//var sites = ["US","DS","AS"]
let directory_name = "/Users/shawnhateley/Projects/Test_Data/MasterFileList";
let masterList = fs.readdirSync(directory_name);
console.log(masterList);

//Get the list of files that match the station
for (var k = 0; k < masterList.length; k++) {
  if (masterList[k].includes(".DS")) continue //skip mac .DS files
  var masterFileName = directory_name + "/" + masterList[k];
  var master = new lineByLine(masterFileName);
  var masterHeader = readHeader(master); //Get the master file header for comparison
  let fileList = {};
  fileList['files']=glob.sync("/Users/shawnhateley/Projects/Test_Data/saltDose/**/" + masterList[k], {});
  console.log(fileList);
  checkFiles(fileList);
}


function checkFiles(fileList){
  for (var i = 0; i < fileList.files.length; i++){ //do for each file in the list
    if (fileList.files[i].includes(".DS")) continue //skip mac .DS files
    var newFile = new lineByLine(fileList.files[i]); //check the header against the master. If it doesn't match, skip
    var newFileHeader = readHeader(newFile);

    if (masterHeader == newFileHeader){ //If the header matches, read the file in, skip the first 4 lines (header) and append to master file
      console.log("Match");
      console.log(fileList.files[i])
      var content = fs.readFileSync(fileList.files[i]).toString().split('\n');

      for (var j = 4; j < content.length; j++){
        fs.appendFileSync(masterFileName,content[j]+'\n')
      }

    } else {
      console.log("No Match");
      console.log(fileList.files[i])
    }
  }
}


function readHeader(fileName){ //function to read the second line (header) from the file passed to it. Data file headers changed in 2016
  let line;
  let lineNumber = 0;

  while (lineNumber < 2) {
      line = fileName.next()
      if (lineNumber == 1){
        //console.log('Line ' + lineNumber + ': ' + line.toString('ascii'));
        return line.toString('ascii');
      }
      lineNumber++;
  }
}
