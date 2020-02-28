var fs = require('fs');
var path = "/Users/shawnhateley/Projects/Test_Data/Iridium Buffer Feb 10 - 24.txt"
var pos = 0
var tmp = 0
var i = -1
var output = []
var search = ["NORM","\\n 16","\\n 17","\\n 18","\\n 18"]

//   //console.log(textByLine)
//   //console.log(text[0][0])
// });

fs.readFile(path,'utf-8',(err, data)  => {
  if (err) throw err;
  //var result = data

var count = (data.match(/NORM/g) || []).length;
console.log(count);
  for (var k=0; k<count; k++) {
    for (var j=0; j<search.length; j++){
      pos = data.indexOf(search[j], i + 10);
      if (pos== -1) break;

      if (j==0){ //get the date or pressure data
        tmp = data.substring(pos+19,pos+36);
      } else {
        tmp = data.substring(pos+18,pos+24);
      }

      if (j==0 && output.includes(tmp)) { //skip duplicates
        j=-1;
        pos = pos + 100
        i = pos;
        continue;
      } else {
        output.push(tmp);
        i = pos;
      }

      if (j==search.length-1) output.push("\n"); //add a newline after each sample

    }

  }

  fs.writeFile('OutputPress.csv', output, (err) => {
// In case of a error throw err.
    if (err) throw err;
  })
});
