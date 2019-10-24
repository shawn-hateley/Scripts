var moment = require("moment");
var mkdirp = require('mkdirp');
var exec = require('child_process').exec;


function takePicture() {

  var name = moment().format('YYYY-MM-DD_HH-mm-ss') + ".jpg"; // create image name with current time
  var folder = moment().format('YYYY-MM-DD'); // create daily folder name
  var folderPath = "/hakai/data/webcam/"+folder;

  mkdirp(folderPath,function(err){ // check if daily folder exists and create it if not
     if (err) console.error(err)
    //path exists unless there was an error
  });

  var cmd = 'ffmpeg -loglevel fatal -rtsp_transport tcp -i "rtsp://10.10.8.5:554/s0" -r 1 -vframes 1 ' + folderPath + "/" + name; //build ffmpeg command with arguments

  //console.log(cmd)
  exec(cmd, function(error, stdout, stderr) {  // call ffmpeg to capture image from stream
    // command output is in stdout
  });
}

setInterval (function(){takePicture()},10000); // take a picture every ten seconds
