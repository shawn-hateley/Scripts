

// Node.js program to demonstrate the   
// buffer.readFloatLE() Method 
      
// Creating a buffer of given size  
let num = 15;
let text = num.toString();

var buffer = new ArrayBuffer(4);
var view = new DataView(buffer);

view.setInt16(2, 57730, false);
view.setInt16(0, 16780, false);

console.log(view.getFloat32(0, false));
console.log(num);
console.log(text);