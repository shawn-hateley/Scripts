var fs = require('fs');
var path = "/Users/shawnhateley/Projects/Test_Data/KCBuoyData20190501-20191126.txt"
var arrayPath = "/Users/shawnhateley/Projects/Test_Data/months.txt"
var text = []

//read txt file into array
fs.readFile(arrayPath,'utf-8',(err, data)  => {
  if (err) throw err;
  text = data.split("\n").map(item => item.trim()); //split the txt file at each new line and trim any excess characters

  //console.log(textByLine)
  //console.log(text)
});

fs.readFile(path,'utf-8',(err, data)  => {
  if (err) throw err;
  //var result = data
  for (i=0; i<text.length; i++){
    //console.log(text[i])
    //console.log(data.indexOf(text[i].trim()))

    var start = data.indexOf(text[i])-17
    var end = data.indexOf(text[i+1])-17
    //console.log(data.length)
    if (i == text.length-1) end = data.length - 1;
    //console.log(start, end)
    var mySubString = data.substring(start,end);

    var name = text[i].replace(/[/]/g,"-") + ".txt"
  //console.log(name)
    fs.writeFile(name, mySubString, (err) => {
// In case of a error throw err.
    if (err) throw err;
  })
}
});
