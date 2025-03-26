var request = require('request');
request.get('https://hecate.hakai.org/saltDose/SSN626DS_AutoDoseEvent.dat.csv', function (error, response, body) {
    if (!error && response.statusCode == 200) {
        var csv = body;
        console.log(csv);
        // Continue with your processing here.
    }
});