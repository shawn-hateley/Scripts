"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.decode = exports.encode = void 0;
const enum_1 = require("./enum");
const encode = (buffer, func, msgLength, originatingIP) => {
    buffer[0] = enum_1.BVLL_TYPE_BACNET_IP;
    buffer[1] = func;
    buffer[2] = (msgLength & 0xff00) >> 8;
    buffer[3] = (msgLength & 0x00ff) >> 0;
    if (originatingIP) {
        if (func !== enum_1.BvlcResultPurpose.FORWARDED_NPDU) {
            throw new Error('Cannot specify originatingIP unless ' +
                'BvlcResultPurpose.FORWARDED_NPDU is used.');
        }
        const [ipstr, portstr] = originatingIP.split(':');
        const port = parseInt(portstr, 10) || enum_1.DEFAULT_BACNET_PORT;
        const ip = ipstr.split('.');
        buffer[4] = parseInt(ip[0], 10);
        buffer[5] = parseInt(ip[1], 10);
        buffer[6] = parseInt(ip[2], 10);
        buffer[7] = parseInt(ip[3], 10);
        buffer[8] = (port & 0xff00) >> 8;
        buffer[9] = (port & 0x00ff) >> 0;
        return 6 + enum_1.BVLC_HEADER_LENGTH;
    }
    else {
        if (func === enum_1.BvlcResultPurpose.FORWARDED_NPDU) {
            throw new Error('Must specify originatingIP if BvlcResultPurpose.FORWARDED_NPDU is used.');
        }
    }
    return enum_1.BVLC_HEADER_LENGTH;
};
exports.encode = encode;
const decode = (buffer, _offset) => {
    if (buffer.length < enum_1.BVLC_HEADER_LENGTH)
        return undefined;
    let len;
    const func = buffer[1];
    const msgLength = (buffer[2] << 8) | (buffer[3] << 0);
    if (msgLength < enum_1.BVLC_HEADER_LENGTH)
        return undefined;
    if (buffer[0] !== enum_1.BVLL_TYPE_BACNET_IP || buffer.length !== msgLength)
        return undefined;
    let originatingIP = null;
    switch (func) {
        case enum_1.BvlcResultPurpose.BVLC_RESULT:
        case enum_1.BvlcResultPurpose.ORIGINAL_UNICAST_NPDU:
        case enum_1.BvlcResultPurpose.ORIGINAL_BROADCAST_NPDU:
        case enum_1.BvlcResultPurpose.DISTRIBUTE_BROADCAST_TO_NETWORK:
        case enum_1.BvlcResultPurpose.REGISTER_FOREIGN_DEVICE:
        case enum_1.BvlcResultPurpose.READ_FOREIGN_DEVICE_TABLE:
        case enum_1.BvlcResultPurpose.DELETE_FOREIGN_DEVICE_TABLE_ENTRY:
        case enum_1.BvlcResultPurpose.READ_BROADCAST_DISTRIBUTION_TABLE:
        case enum_1.BvlcResultPurpose.WRITE_BROADCAST_DISTRIBUTION_TABLE:
        case enum_1.BvlcResultPurpose.READ_BROADCAST_DISTRIBUTION_TABLE_ACK:
        case enum_1.BvlcResultPurpose.READ_FOREIGN_DEVICE_TABLE_ACK:
            len = 4;
            break;
        case enum_1.BvlcResultPurpose.FORWARDED_NPDU:
            if (msgLength < 10)
                return undefined;
            const port = (buffer[8] << 8) | buffer[9];
            originatingIP = buffer.slice(4, 8).join('.');
            if (port !== enum_1.DEFAULT_BACNET_PORT) {
                originatingIP += `:${port}`;
            }
            len = 10;
            break;
        case enum_1.BvlcResultPurpose.SECURE_BVLL:
            return undefined;
        default:
            return undefined;
    }
    return {
        len,
        func,
        msgLength,
        originatingIP,
    };
};
exports.decode = decode;
//# sourceMappingURL=bvlc.js.map