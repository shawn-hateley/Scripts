"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.decodeAbort = exports.encodeAbort = exports.decodeError = exports.encodeError = exports.decodeResult = exports.encodeResult = exports.decodeSegmentAck = exports.encodeSegmentAck = exports.decodeComplexAck = exports.encodeComplexAck = exports.decodeSimpleAck = exports.encodeSimpleAck = exports.decodeUnconfirmedServiceRequest = exports.encodeUnconfirmedServiceRequest = exports.decodeConfirmedServiceRequest = exports.encodeConfirmedServiceRequest = exports.getDecodedInvokeId = exports.setDecodedType = exports.getDecodedType = void 0;
const baAsn1 = __importStar(require("./asn1"));
const enum_1 = require("./enum");
const getDecodedType = (buffer, offset) => {
    return buffer[offset];
};
exports.getDecodedType = getDecodedType;
const setDecodedType = (buffer, offset, type) => {
    buffer[offset] = type;
};
exports.setDecodedType = setDecodedType;
const getDecodedInvokeId = (buffer, offset) => {
    const type = (0, exports.getDecodedType)(buffer, offset);
    switch (type & enum_1.PDU_TYPE_MASK) {
        case enum_1.PduType.SIMPLE_ACK:
        case enum_1.PduType.COMPLEX_ACK:
        case enum_1.PduType.ERROR:
        case enum_1.PduType.REJECT:
        case enum_1.PduType.ABORT:
            return buffer[offset + 1];
        case enum_1.PduType.CONFIRMED_REQUEST:
            return buffer[offset + 2];
        default:
            return undefined;
    }
};
exports.getDecodedInvokeId = getDecodedInvokeId;
const encodeConfirmedServiceRequest = (buffer, type, service, maxSegments, maxApdu, invokeId, sequencenumber, proposedWindowSize) => {
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = maxSegments | maxApdu;
    buffer.buffer[buffer.offset++] = invokeId;
    if ((type & enum_1.PduConReqBit.SEGMENTED_MESSAGE) > 0) {
        if (sequencenumber === undefined) {
            throw new Error('sequencenumber is undefined');
        }
        buffer.buffer[buffer.offset++] = sequencenumber;
        if (proposedWindowSize === undefined) {
            throw new Error('proposedWindowSize is undefined');
        }
        buffer.buffer[buffer.offset++] = proposedWindowSize;
    }
    buffer.buffer[buffer.offset++] = service;
};
exports.encodeConfirmedServiceRequest = encodeConfirmedServiceRequest;
const decodeConfirmedServiceRequest = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const maxSegments = buffer[offset] & 0xf0;
    const maxApdu = buffer[offset++] & 0x0f;
    const invokeId = buffer[offset++];
    let sequencenumber = 0;
    let proposedWindowNumber = 0;
    if ((type & enum_1.PduConReqBit.SEGMENTED_MESSAGE) > 0) {
        sequencenumber = buffer[offset++];
        proposedWindowNumber = buffer[offset++];
    }
    const service = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        service,
        maxSegments,
        maxApdu,
        invokeId,
        sequencenumber,
        proposedWindowNumber,
    };
};
exports.decodeConfirmedServiceRequest = decodeConfirmedServiceRequest;
const encodeUnconfirmedServiceRequest = (buffer, type, service) => {
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = service;
};
exports.encodeUnconfirmedServiceRequest = encodeUnconfirmedServiceRequest;
const decodeUnconfirmedServiceRequest = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const service = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        service,
    };
};
exports.decodeUnconfirmedServiceRequest = decodeUnconfirmedServiceRequest;
const encodeSimpleAck = (buffer, type, service, invokeId) => {
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = invokeId;
    buffer.buffer[buffer.offset++] = service;
};
exports.encodeSimpleAck = encodeSimpleAck;
const decodeSimpleAck = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const invokeId = buffer[offset++];
    const service = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        service,
        invokeId,
    };
};
exports.decodeSimpleAck = decodeSimpleAck;
const encodeComplexAck = (buffer, type, service, invokeId, sequencenumber, proposedWindowNumber) => {
    let len = 3;
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = invokeId;
    if ((type & enum_1.PduConReqBit.SEGMENTED_MESSAGE) > 0) {
        if (sequencenumber === undefined) {
            throw new Error('sequencenumber is undefined');
        }
        buffer.buffer[buffer.offset++] = sequencenumber;
        if (proposedWindowNumber === undefined) {
            throw new Error('proposedWindowNumber is undefined');
        }
        buffer.buffer[buffer.offset++] = proposedWindowNumber;
        len += 2;
    }
    buffer.buffer[buffer.offset++] = service;
    return len;
};
exports.encodeComplexAck = encodeComplexAck;
const decodeComplexAck = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const invokeId = buffer[offset++];
    let sequencenumber = 0;
    let proposedWindowNumber = 0;
    if ((type & enum_1.PduConReqBit.SEGMENTED_MESSAGE) > 0) {
        sequencenumber = buffer[offset++];
        proposedWindowNumber = buffer[offset++];
    }
    const service = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        service,
        invokeId,
        sequencenumber,
        proposedWindowNumber,
    };
};
exports.decodeComplexAck = decodeComplexAck;
const encodeSegmentAck = (buffer, type, originalInvokeId, sequencenumber, actualWindowSize) => {
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = originalInvokeId;
    buffer.buffer[buffer.offset++] = sequencenumber;
    buffer.buffer[buffer.offset++] = actualWindowSize;
};
exports.encodeSegmentAck = encodeSegmentAck;
const decodeSegmentAck = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const originalInvokeId = buffer[offset++];
    const sequencenumber = buffer[offset++];
    const actualWindowSize = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        originalInvokeId,
        sequencenumber,
        actualWindowSize,
    };
};
exports.decodeSegmentAck = decodeSegmentAck;
const encodeResult = (buffer, resultCode) => {
    baAsn1.encodeUnsigned(buffer, resultCode, 2);
};
exports.encodeResult = encodeResult;
const decodeResult = (buffer, offset) => {
    const orgOffset = offset;
    const decode = baAsn1.decodeUnsigned(buffer, offset, 2);
    offset += decode.len;
    return {
        len: offset - orgOffset,
        resultCode: decode.value,
    };
};
exports.decodeResult = decodeResult;
const encodeError = (buffer, type, service, invokeId) => {
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = invokeId;
    buffer.buffer[buffer.offset++] = service;
};
exports.encodeError = encodeError;
const decodeError = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const invokeId = buffer[offset++];
    const service = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        service,
        invokeId,
    };
};
exports.decodeError = decodeError;
const encodeAbort = (buffer, type, invokeId, reason) => {
    buffer.buffer[buffer.offset++] = type;
    buffer.buffer[buffer.offset++] = invokeId;
    buffer.buffer[buffer.offset++] = reason;
};
exports.encodeAbort = encodeAbort;
const decodeAbort = (buffer, offset) => {
    const orgOffset = offset;
    const type = buffer[offset++];
    const invokeId = buffer[offset++];
    const reason = buffer[offset++];
    return {
        len: offset - orgOffset,
        type,
        invokeId,
        reason,
    };
};
exports.decodeAbort = decodeAbort;
//# sourceMappingURL=apdu.js.map