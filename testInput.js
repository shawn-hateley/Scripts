var Gpio = require('onoff').Gpio;

var doorStatus = new Gpio(5, 'in');

doorStatus.read((err,value) => {
        if (err) {
                throw err;
        }
        console.log(value);
});