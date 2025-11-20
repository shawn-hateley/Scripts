let Controller = require('node-pid-controller');

let ctr = new Controller({
  k_p: 0.25,
  k_i: 0.01,
  k_d: 0.01,
  dt: 1
});

ctr.setTarget(120); // 120km/h

let correction = ctr.update(110); // 110km/h is the current speed

let goalReached = false
while (!goalReached) {
  let output = 115
  let input  = ctr.update(output);
  applyInputToActuator(input);
  goalReached = (input === 0); // in the case of continuous control, you let this variable 'false'
}