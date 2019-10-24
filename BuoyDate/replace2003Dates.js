var fs = require('fs');
var path = "/Users/shawnhateley/Documents/Sensor Network/Mooring Buoys/KC Buoy Data/mapco2flashdata_05192019_0190_edited.txt"
var arrayPath = "/Users/shawnhateley/Projects/Test_Data/buoyDates.txt"
var text = []


fs.readFile(arrayPath,'utf-8',(err, data)  => {
  if (err) throw err;
  var textByLine = data.split("\n")
  for (i=0; i<textByLine.length; i++){
    text.push(textByLine[i].split(","));
  }
  //console.log(textByLine)
  //console.log(text[0][0])
});

fs.readFile(path,'utf-8',(err, data)  => {
  if (err) throw err;
  //var result = data
  for (i=0; i<text.length; i++){
  //  console.log(text[i][0])
    data = data.replace(text[i][0],text[i][1].trim()) //need to remove the newline at the end of the date
  //  console.log(text[i][1])
  }
  fs.writeFile('Output.txt', data, (err) => {
// In case of a error throw err.
    if (err) throw err;
  })
});
