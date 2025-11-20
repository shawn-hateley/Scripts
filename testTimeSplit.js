
function testTimeSplit(value){
    var split = value.split(' '),     
        MONTHS_LIST = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],     
        month_index = ('0' + (MONTHS_LIST.indexOf(split[0]) + 1)).slice(-2),     
        ISOdate = split[3] + '-' + month_index + '-' + split[1] + 'T' + split[2],     
        now = Date.now(); 
        
    //console.log(month_index)
    //console.log(ISOdate)
    //console.log(now)
    //return parseInt((Date.parse(ISOdate) - now) / 1000);
    return (Date.parse(ISOdate));
}

function buildTime(date, time){
    //2026-02-12T12:33:56
    time = time.split(' ')[0]
    var ISOdate = date + "T" + time
    console.log(ISOdate)
    return (Date.parse(ISOdate))
}
//console.log(testTimeSplit("Feb 12 12:33:56 2026 GMT"))
console.log(buildTime("2025-11-13", "07:49:36 AM"))