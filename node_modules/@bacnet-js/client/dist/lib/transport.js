"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dgram_1 = require("dgram");
const EventTypes_1 = require("./EventTypes");
const enum_1 = require("./enum");
const debug_1 = __importDefault(require("debug"));
const debug = (0, debug_1.default)('bacnet:transport:debug');
const trace = (0, debug_1.default)('bacnet:transport:trace');
class Transport extends EventTypes_1.TypedEventEmitter {
    _settings;
    _server;
    _lastSendMessages = {};
    ownAddress = {
        address: '',
        port: 0,
    };
    constructor(settings) {
        super();
        this._settings = settings;
        this._server = (0, dgram_1.createSocket)({
            type: 'udp4',
            reuseAddr: settings.reuseAddr,
        });
        this._server.on('message', (msg, rinfo) => {
            if (this.ownAddress.port === rinfo.port) {
                for (const [messageKey, earlierSentBuffer] of Object.entries(this._lastSendMessages)) {
                    if (msg.equals(earlierSentBuffer)) {
                        debug(`server IGNORE message from ${rinfo.address}:${rinfo.port} (${messageKey}): ${msg.toString('hex')}`);
                        return;
                    }
                }
            }
            debug(`server got message from ${rinfo.address}:${rinfo.port}: ${msg.toString('hex')}`);
            this.emit('message', msg, rinfo.address +
                (rinfo.port === enum_1.DEFAULT_BACNET_PORT
                    ? ''
                    : `:${rinfo.port}`));
        });
        this._server.on('listening', () => {
            this.ownAddress = this._server.address();
            debug(`server listening on ${this.ownAddress.address}:${this.ownAddress.port}`);
            this.emit('listening', this.ownAddress);
        });
        this._server.on('error', (err) => {
            debug('transport error', err.message);
            this.emit('error', err);
        });
        this._server.on('close', () => {
            debug('transport closed');
            this.emit('close');
        });
    }
    getBroadcastAddress() {
        return this._settings.broadcastAddress;
    }
    getMaxPayload() {
        return 1482;
    }
    send(buffer, offset, receiver) {
        if (!receiver) {
            receiver = this.getBroadcastAddress();
            const dataToSend = Buffer.alloc(offset);
            const hrTime = process.hrtime();
            const messageKey = hrTime[0] * 1000000000 + hrTime[1];
            buffer.copy(dataToSend, 0, 0, offset);
            this._lastSendMessages[messageKey] = dataToSend;
            setTimeout(() => {
                delete this._lastSendMessages[messageKey];
            }, 10000);
        }
        const [address, port] = receiver.split(':');
        debug(`Send packet to ${receiver}: ${buffer
            .toString('hex')
            .substring(0, offset * 2)}`);
        this._server.send(buffer, 0, offset, parseInt(port) || enum_1.DEFAULT_BACNET_PORT, address);
    }
    open() {
        this._server.bind(this._settings.port || enum_1.DEFAULT_BACNET_PORT, this._settings.interface, () => {
            this._server.setBroadcast(true);
        });
    }
    close() {
        this._server.close();
    }
}
exports.default = Transport;
//# sourceMappingURL=transport.js.map