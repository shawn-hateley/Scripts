#!/usr/bin/node

'use strict'

const { Buffer } = require('node:buffer');

const modbus = require('jsmodbus')
const net = require('net')
const socket = new net.Socket()
const options = {
  'host': '10.12.254.19',
  'port': '502'
}
const client = new modbus.client.TCP(socket)

socket.on('connect', function () {
  client.readHoldingRegisters(9219, 2)
    .then(function (resp) {
      var buf = Buffer.from(resp.response._body.valuesAsBuffer)
      buf.swap16(); //change from DCBA to CDAB
      console.log(buf.readFloatLE(0));
      socket.end()
    }).catch(function () {
      console.error(require('util').inspect(arguments, {
        depth: null
      }))
      socket.end()
    })
})

socket.on('error', console.error)
socket.connect(options)
