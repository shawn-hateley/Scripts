// using spawn
let spawn = require('child_process').spawn;
 
console.log('main process started.');
 
let script = spawn('node', ['webcam.js']);
 
script.stdout.on('data', (data) => {
 
    // data is a Buffer
    // log a conversion to a string that is one less byte
    // this is drop the line feed.
    console.log(data.slice(0,data.length-1).toString('utf8'));
 
});
 
// start time
let st = new Date();
 
setInterval(function () {
 
    let time = new Date() - st;
 
    // if time is over 5 secs, and the script has not been killed...
    if (time > 5000 && !script.killed) {
 
        // pause and kill script
        script.stdin.pause();
        script.kill();
        console.log('child killed');
 
    }
 
    // After ten seconds kill this main script
    if (time > 30000) {
 
        console.log('ending main process');
        process.exit();
 
    }
 
    // log what this script is doing
    console.log('main: ' + time);
 
}, 1000);