var moment = require("moment");
var mkdirp = require('mkdirp');
var exec = require('child_process').exec;


function takePicture() {

  var name = moment().format('YYYY-MM-DD_HH-mm-ss') + ".jpg"; // create image name with current time
  var folder = moment().format('YYYY-MM-DD'); // create daily folder name
  var folderPath = "/media/usbstick/webcam/"+folder;

  //console.log(folder + folderPath);

  mkdirp(folderPath);

  var cmd = 'ffmpeg -loglevel fatal -rtsp_transport tcp -i "rtsp://192.168.1.91:8554/unicast" -r 1 -vframes 1 -stimeout 5000 ' + folderPath + "/" + name; //build ffmpeg command with arguments

  console.log(cmd)
  exec(cmd, function(error, stdout, stderr) {  // call ffmpeg to capture image from stream
    // command output is in stdout
  });
}

setInterval (function(){takePicture()},10000); // take a picture every ten seconds

//use the spawn code to call the ffmpeg cmd as a child process. Need to pass arguments?