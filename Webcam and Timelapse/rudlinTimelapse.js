var exec = require('child_process').exec;
var moment = require("moment");
var mkdirp = require('mkdirp');



function takePicture() {

  
  var yesterday = moment().subtract(1, "days").format('YYYY-MM-DD'); // get yesterdays folder name
  var folderPath = "/media/usbstick/webcam/"+yesterday;
  var output = "/media/usbstick/timelapse/" + yesterday +".mp4"; // create image name with yesterdays date
  
  //console.log(folder + folderPath);
  //mkdirp(folderPath,function(err){ // check if daily folder exists and create it if not
    // if (err) console.error(err)
    //path exists unless there was an error
  //});
  //mkdirp(folderPath);
  var cmd = "ffmpeg -framerate 5 -pattern_type glob -i " + folderPath + "/" + "*.jpg -c:v libx264 -r 30" + output;  //build ffmpeg command with arguments
  //var cmd = 'ffmpeg -loglevel fatal -rtsp_transport tcp -i "rtsp://10.10.8.5:554/s0" -r 1 -vframes 1 ' + folderPath + "/" + name; //build ffmpeg command with arguments

  console.log(cmd)
  //exec(cmd, function(error, stdout, stderr) {  // call ffmpeg to capture image from stream
    // command output is in stdout
  //});
}

//setInterval (function(){takePicture()},10000); // take a picture every ten seconds
takePicture;