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
exports.decodeBacnetTime = exports.decodeApplicationDate = exports.decodeDate = exports.decodeBitstring = exports.decodeCharacterString = exports.decodeOctetString = exports.decodeReal = exports.decodeSigned = exports.decodeReadAccessResult = exports.encodeReadAccessResult = exports.bacappDecodeApplicationData = exports.decodeTagNumberAndValue = exports.decodeObjectId = exports.decodeIsOpeningTag = exports.decodeIsClosingTag = exports.decodeIsClosingTagNumber = exports.decodeIsOpeningTagNumber = exports.decodeIsContextTag = exports.decodeTagNumber = exports.encodeContextSigned = exports.encodeContextBitstring = exports.bacappEncodePropertyState = exports.bacappEncodeContextDeviceObjPropertyRef = exports.bacappEncodeApplicationData = exports.encodeContextBoolean = exports.encodeReadAccessSpecification = exports.encodeClosingTag = exports.encodeOpeningTag = exports.encodeContextObjectId = exports.encodeApplicationTime = exports.encodeApplicationDate = exports.encodeBacnetDate = exports.encodeApplicationBitstring = exports.encodeApplicationSigned = exports.encodeApplicationEnumerated = exports.encodeApplicationUnsigned = exports.encodeApplicationObjectId = exports.encodeApplicationBoolean = exports.encodeApplicationOctetString = exports.encodeContextEnumerated = exports.encodeContextUnsigned = exports.encodeContextReal = exports.encodeTag = exports.encodeBacnetObjectId = exports.decodeEnumerated = exports.decodeUnsigned = exports.encodeUnsigned = exports.ZERO_DATE = exports.MAX_YEARS = exports.START_YEAR = void 0;
exports.encodeContextCharacterString = exports.encodeApplicationCharacterString = exports.decodeContextObjectId = exports.decodeContextCharacterString = exports.bacappEncodeContextTimestamp = exports.bacappEncodeTimestamp = exports.decodeRange = exports.decodeCalendarDatelist = exports.decodeScheduleEffectivePeriod = exports.decodeExceptionSchedule = exports.decodeWeeklySchedule = exports.decodeReadAccessSpecification = exports.decodeApplicationTime = void 0;
const iconv = __importStar(require("iconv-lite"));
const enum_1 = require("./enum");
exports.START_YEAR = 1900;
exports.MAX_YEARS = 256;
exports.ZERO_DATE = new Date(exports.START_YEAR, 0, 1);
const getBuffer = () => ({
    buffer: Buffer.alloc(1472),
    offset: 0,
});
const getSignedLength = (value) => {
    if (value >= -128 && value < 128)
        return 1;
    if (value >= -32768 && value < 32768)
        return 2;
    if (value > -8388608 && value < 8388608)
        return 3;
    return 4;
};
const getUnsignedLength = (value) => {
    if (value < 0x100)
        return 1;
    if (value < 0x10000)
        return 2;
    if (value < 0x1000000)
        return 3;
    return 4;
};
const getEncodingType = (encoding, decodingBuffer, decodingOffset) => {
    switch (encoding) {
        case enum_1.CharacterStringEncoding.UCS_2:
            if (decodingBuffer &&
                decodingBuffer[decodingOffset] === 0xff &&
                decodingBuffer[decodingOffset + 1] === 0xfe) {
                return 'ucs2';
            }
            return 'UTF-16BE';
        case enum_1.CharacterStringEncoding.ISO_8859_1:
            return 'latin1';
        case enum_1.CharacterStringEncoding.MICROSOFT_DBCS:
            return 'cp850';
        case enum_1.CharacterStringEncoding.JIS_X_0208:
            return 'Shift_JIS';
        default:
            return 'utf8';
    }
};
const encodeUnsigned = (buffer, value, length) => {
    buffer.buffer.writeUIntBE(value, buffer.offset, length);
    buffer.offset += length;
};
exports.encodeUnsigned = encodeUnsigned;
const encodeBacnetUnsigned = (buffer, value) => {
    (0, exports.encodeUnsigned)(buffer, value, getUnsignedLength(value));
};
const encodeSigned = (buffer, value, length) => {
    buffer.buffer.writeIntBE(value, buffer.offset, length);
    buffer.offset += length;
};
const encodeBacnetSigned = (buffer, value) => {
    encodeSigned(buffer, value, getSignedLength(value));
};
const encodeBacnetReal = (buffer, value) => {
    buffer.buffer.writeFloatBE(value, buffer.offset);
    buffer.offset += 4;
};
const encodeBacnetDouble = (buffer, value) => {
    buffer.buffer.writeDoubleBE(value, buffer.offset);
    buffer.offset += 8;
};
const decodeUnsigned = (buffer, offset, length) => {
    if (length === 0) {
        return {
            len: 0,
            value: 0,
        };
    }
    return {
        len: length,
        value: buffer.readUIntBE(offset, length),
    };
};
exports.decodeUnsigned = decodeUnsigned;
const decodeEnumerated = (buffer, offset, lenValue) => {
    return (0, exports.decodeUnsigned)(buffer, offset, lenValue);
};
exports.decodeEnumerated = decodeEnumerated;
const encodeBacnetObjectId = (buffer, objectType, instance) => {
    const value = (((objectType & enum_1.ASN1_MAX_OBJECT) << enum_1.ASN1_INSTANCE_BITS) |
        (instance & enum_1.ASN1_MAX_INSTANCE)) >>>
        0;
    (0, exports.encodeUnsigned)(buffer, value, 4);
};
exports.encodeBacnetObjectId = encodeBacnetObjectId;
const encodeTag = (buffer, tagNumber, contextSpecific, lenValueType) => {
    let len = 1;
    const tmp = new Array(3);
    tmp[0] = 0;
    if (contextSpecific) {
        tmp[0] |= 0x8;
    }
    if (tagNumber <= 14) {
        tmp[0] |= tagNumber << 4;
    }
    else {
        tmp[0] |= 0xf0;
        tmp[1] = tagNumber;
        len++;
    }
    if (lenValueType <= 4) {
        tmp[0] |= lenValueType;
        const tmpBuffer = Buffer.from(tmp);
        tmpBuffer.copy(buffer.buffer, buffer.offset, 0, len);
        buffer.offset += len;
    }
    else {
        tmp[0] |= 5;
        if (lenValueType <= 253) {
            tmp[len++] = lenValueType;
            const tmpBuffer = Buffer.from(tmp);
            tmpBuffer.copy(buffer.buffer, buffer.offset, 0, len);
            buffer.offset += len;
        }
        else if (lenValueType <= 65535) {
            tmp[len++] = 254;
            const tmpBuffer = Buffer.from(tmp);
            tmpBuffer.copy(buffer.buffer, buffer.offset, 0, len);
            buffer.offset += len;
            (0, exports.encodeUnsigned)(buffer, lenValueType, 2);
        }
        else {
            tmp[len++] = 255;
            const tmpBuffer = Buffer.from(tmp);
            tmpBuffer.copy(buffer.buffer, buffer.offset, 0, len);
            buffer.offset += len;
            (0, exports.encodeUnsigned)(buffer, lenValueType, 4);
        }
    }
};
exports.encodeTag = encodeTag;
const encodeBacnetEnumerated = (buffer, value) => {
    encodeBacnetUnsigned(buffer, value);
};
const isExtendedTagNumber = (x) => {
    return (x & 0xf0) === 0xf0;
};
const isExtendedValue = (x) => {
    return (x & 0x07) === 5;
};
const isContextSpecific = (x) => {
    return (x & 0x8) === 0x8;
};
const isOpeningTag = (x) => {
    return (x & 0x07) === 6;
};
const isClosingTag = (x) => {
    return (x & 0x07) === 7;
};
const encodeContextReal = (buffer, tagNumber, value) => {
    (0, exports.encodeTag)(buffer, tagNumber, true, 4);
    encodeBacnetReal(buffer, value);
};
exports.encodeContextReal = encodeContextReal;
const encodeContextUnsigned = (buffer, tagNumber, value) => {
    (0, exports.encodeTag)(buffer, tagNumber, true, getUnsignedLength(value));
    encodeBacnetUnsigned(buffer, value);
};
exports.encodeContextUnsigned = encodeContextUnsigned;
const encodeContextEnumerated = (buffer, tagNumber, value) => {
    (0, exports.encodeContextUnsigned)(buffer, tagNumber, value);
};
exports.encodeContextEnumerated = encodeContextEnumerated;
const encodeOctetString = (buffer, octetString, octetOffset, octetCount) => {
    if (octetString) {
        for (let i = octetOffset; i < octetOffset + octetCount; i++) {
            buffer.buffer[buffer.offset++] = octetString[i];
        }
    }
};
const encodeApplicationOctetString = (buffer, octetString, octetOffset, octetCount) => {
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.OCTET_STRING, false, octetCount);
    encodeOctetString(buffer, octetString, octetOffset, octetCount);
};
exports.encodeApplicationOctetString = encodeApplicationOctetString;
const encodeApplicationNull = (buffer) => {
    buffer.buffer[buffer.offset++] = enum_1.ApplicationTag.NULL;
};
const encodeApplicationBoolean = (buffer, booleanValue) => {
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.BOOLEAN, false, booleanValue ? 1 : 0);
};
exports.encodeApplicationBoolean = encodeApplicationBoolean;
const encodeApplicationReal = (buffer, value) => {
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.REAL, false, 4);
    encodeBacnetReal(buffer, value);
};
const encodeApplicationDouble = (buffer, value) => {
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.DOUBLE, false, 8);
    encodeBacnetDouble(buffer, value);
};
const bitstringBytesUsed = (bitString) => {
    let len = 0;
    if (bitString.bitsUsed > 0) {
        const lastBit = bitString.bitsUsed - 1;
        const usedBytes = lastBit / 8 + 1;
        len = Math.floor(usedBytes);
    }
    return len;
};
const encodeApplicationObjectId = (buffer, objectType, instance) => {
    const tmp = getBuffer();
    (0, exports.encodeBacnetObjectId)(tmp, objectType, instance);
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.OBJECTIDENTIFIER, false, tmp.offset);
    tmp.buffer.copy(buffer.buffer, buffer.offset, 0, tmp.offset);
    buffer.offset += tmp.offset;
};
exports.encodeApplicationObjectId = encodeApplicationObjectId;
const encodeApplicationUnsigned = (buffer, value) => {
    const tmp = getBuffer();
    encodeBacnetUnsigned(tmp, value);
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.UNSIGNED_INTEGER, false, tmp.offset);
    tmp.buffer.copy(buffer.buffer, buffer.offset, 0, tmp.offset);
    buffer.offset += tmp.offset;
};
exports.encodeApplicationUnsigned = encodeApplicationUnsigned;
const encodeApplicationEnumerated = (buffer, value) => {
    const tmp = getBuffer();
    encodeBacnetEnumerated(tmp, value);
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.ENUMERATED, false, tmp.offset);
    tmp.buffer.copy(buffer.buffer, buffer.offset, 0, tmp.offset);
    buffer.offset += tmp.offset;
};
exports.encodeApplicationEnumerated = encodeApplicationEnumerated;
const encodeApplicationSigned = (buffer, value) => {
    const tmp = getBuffer();
    encodeBacnetSigned(tmp, value);
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.SIGNED_INTEGER, false, tmp.offset);
    tmp.buffer.copy(buffer.buffer, buffer.offset, 0, tmp.offset);
    buffer.offset += tmp.offset;
};
exports.encodeApplicationSigned = encodeApplicationSigned;
const byteReverseBits = (inByte) => {
    let outByte = 0;
    if ((inByte & 1) > 0) {
        outByte |= 0x80;
    }
    if ((inByte & 2) > 0) {
        outByte |= 0x40;
    }
    if ((inByte & 4) > 0) {
        outByte |= 0x20;
    }
    if ((inByte & 8) > 0) {
        outByte |= 0x10;
    }
    if ((inByte & 16) > 0) {
        outByte |= 0x8;
    }
    if ((inByte & 32) > 0) {
        outByte |= 0x4;
    }
    if ((inByte & 64) > 0) {
        outByte |= 0x2;
    }
    if ((inByte & 128) > 0) {
        outByte |= 1;
    }
    return outByte;
};
const bitstringOctet = (bitString, octetIndex) => {
    let octet = 0;
    if (bitString.value && octetIndex < enum_1.ASN1_MAX_BITSTRING_BYTES) {
        octet = bitString.value[octetIndex];
    }
    return octet;
};
const encodeBitstring = (buffer, bitString) => {
    if (bitString.bitsUsed === 0) {
        buffer.buffer[buffer.offset++] = 0;
    }
    else {
        const usedBytes = bitstringBytesUsed(bitString);
        const remainingUsedBits = bitString.bitsUsed - (usedBytes - 1) * 8;
        buffer.buffer[buffer.offset++] = 8 - remainingUsedBits;
        for (let i = 0; i < usedBytes; i++) {
            buffer.buffer[buffer.offset++] = byteReverseBits(bitstringOctet(bitString, i));
        }
    }
};
const encodeApplicationBitstring = (buffer, bitString) => {
    let bitStringEncodedLength = 1;
    bitStringEncodedLength += bitstringBytesUsed(bitString);
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.BIT_STRING, false, bitStringEncodedLength);
    encodeBitstring(buffer, bitString);
};
exports.encodeApplicationBitstring = encodeApplicationBitstring;
const encodeBacnetDate = (buffer, value) => {
    if (value === exports.ZERO_DATE) {
        buffer.buffer[buffer.offset++] = 0xff;
        buffer.buffer[buffer.offset++] = 0xff;
        buffer.buffer[buffer.offset++] = 0xff;
        buffer.buffer[buffer.offset++] = 0xff;
        return;
    }
    if (value.getFullYear() >= exports.START_YEAR) {
        buffer.buffer[buffer.offset++] = value.getFullYear() - exports.START_YEAR;
    }
    else if (value.getFullYear() < exports.MAX_YEARS) {
        buffer.buffer[buffer.offset++] = value.getFullYear();
    }
    else {
        throw new Error(`invalid year: ${value.getFullYear()}`);
    }
    buffer.buffer[buffer.offset++] = value.getMonth() + 1;
    buffer.buffer[buffer.offset++] = value.getDate();
    buffer.buffer[buffer.offset++] = value.getDay() === 0 ? 7 : value.getDay();
};
exports.encodeBacnetDate = encodeBacnetDate;
const validateRawDateByte = (name, value, min, max) => {
    if (!Number.isInteger(value) || value < min || value > max) {
        throw new Error(`invalid raw date ${name}: ${value}`);
    }
};
const encodeRawBacnetDate = (buffer, value) => {
    validateRawDateByte('year', value.year, 0, 0xff);
    if (value.month !== 0xff) {
        validateRawDateByte('month', value.month, 1, 14);
    }
    if (value.day !== 0xff) {
        validateRawDateByte('day', value.day, 1, 34);
    }
    if (value.wday !== 0xff) {
        validateRawDateByte('wday', value.wday, 1, 7);
    }
    buffer.buffer[buffer.offset++] = value.year;
    buffer.buffer[buffer.offset++] = value.month;
    buffer.buffer[buffer.offset++] = value.day;
    buffer.buffer[buffer.offset++] = value.wday;
};
const encodeApplicationDate = (buffer, value) => {
    const normalized = typeof value === 'number' ? new Date(value) : value;
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.DATE, false, 4);
    if (normalized instanceof Date) {
        (0, exports.encodeBacnetDate)(buffer, normalized);
    }
    else {
        encodeRawBacnetDate(buffer, normalized);
    }
};
exports.encodeApplicationDate = encodeApplicationDate;
const encodeBacnetTime = (buffer, value) => {
    buffer.buffer[buffer.offset++] = value.getHours();
    buffer.buffer[buffer.offset++] = value.getMinutes();
    buffer.buffer[buffer.offset++] = value.getSeconds();
    buffer.buffer[buffer.offset++] = value.getMilliseconds() / 10;
};
const encodeApplicationTime = (buffer, value) => {
    if (typeof value === 'number' && !Number.isFinite(value)) {
        throw new Error(`invalid timestamp: ${value}`);
    }
    const normalized = typeof value === 'number' ? new Date(value) : value;
    if (Number.isNaN(normalized.getTime())) {
        throw new Error(`invalid time: ${value}`);
    }
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.TIME, false, 4);
    encodeBacnetTime(buffer, normalized);
};
exports.encodeApplicationTime = encodeApplicationTime;
const bacappEncodeDatetime = (buffer, value) => {
    if (value !== exports.ZERO_DATE) {
        (0, exports.encodeApplicationDate)(buffer, value);
        (0, exports.encodeApplicationTime)(buffer, value);
    }
};
const encodeContextObjectId = (buffer, tagNumber, objectType, instance) => {
    (0, exports.encodeTag)(buffer, tagNumber, true, 4);
    (0, exports.encodeBacnetObjectId)(buffer, objectType, instance);
};
exports.encodeContextObjectId = encodeContextObjectId;
const encodeOpeningTag = (buffer, tagNumber) => {
    let len = 1;
    const tmp = new Array(2);
    tmp[0] = 0x8;
    if (tagNumber <= 14) {
        tmp[0] |= tagNumber << 4;
    }
    else {
        tmp[0] |= 0xf0;
        tmp[1] = tagNumber;
        len++;
    }
    tmp[0] |= 6;
    Buffer.from(tmp).copy(buffer.buffer, buffer.offset, 0, len);
    buffer.offset += len;
};
exports.encodeOpeningTag = encodeOpeningTag;
const encodeClosingTag = (buffer, tagNumber) => {
    let len = 1;
    const tmp = new Array(2);
    tmp[0] = 0x8;
    if (tagNumber <= 14) {
        tmp[0] |= tagNumber << 4;
    }
    else {
        tmp[0] |= 0xf0;
        tmp[1] = tagNumber;
        len++;
    }
    tmp[0] |= 7;
    Buffer.from(tmp).copy(buffer.buffer, buffer.offset, 0, len);
    buffer.offset += len;
};
exports.encodeClosingTag = encodeClosingTag;
const encodeReadAccessSpecification = (buffer, value) => {
    (0, exports.encodeContextObjectId)(buffer, 0, value.objectId.type, value.objectId.instance);
    (0, exports.encodeOpeningTag)(buffer, 1);
    value.properties.forEach((p) => {
        (0, exports.encodeContextEnumerated)(buffer, 0, p.id);
        if (p.index && p.index !== enum_1.ASN1_ARRAY_ALL) {
            (0, exports.encodeContextUnsigned)(buffer, 1, p.index);
        }
    });
    (0, exports.encodeClosingTag)(buffer, 1);
};
exports.encodeReadAccessSpecification = encodeReadAccessSpecification;
const encodeContextBoolean = (buffer, tagNumber, booleanValue) => {
    (0, exports.encodeTag)(buffer, tagNumber, true, 1);
    buffer.buffer.writeUInt8(booleanValue ? 1 : 0, buffer.offset);
    buffer.offset += 1;
};
exports.encodeContextBoolean = encodeContextBoolean;
const encodeCovSubscription = (buffer, value) => {
    (0, exports.encodeOpeningTag)(buffer, 0);
    (0, exports.encodeOpeningTag)(buffer, 0);
    (0, exports.encodeOpeningTag)(buffer, 1);
    (0, exports.encodeApplicationUnsigned)(buffer, value.recipient.network);
    if (value.recipient.network === 0xffff) {
        (0, exports.encodeApplicationOctetString)(buffer, [0], 0, 0);
    }
    else {
        (0, exports.encodeApplicationOctetString)(buffer, value.recipient.address, 0, value.recipient.address.length);
    }
    (0, exports.encodeClosingTag)(buffer, 1);
    (0, exports.encodeClosingTag)(buffer, 0);
    (0, exports.encodeContextUnsigned)(buffer, 1, value.subscriptionProcessId);
    (0, exports.encodeClosingTag)(buffer, 0);
    (0, exports.encodeOpeningTag)(buffer, 1);
    (0, exports.encodeContextObjectId)(buffer, 0, value.monitoredObjectId.type, value.monitoredObjectId.instance);
    (0, exports.encodeContextEnumerated)(buffer, 1, value.monitoredProperty.id);
    if (value.monitoredProperty.index !== enum_1.ASN1_ARRAY_ALL) {
        (0, exports.encodeContextUnsigned)(buffer, 2, value.monitoredProperty.index);
    }
    (0, exports.encodeClosingTag)(buffer, 1);
    (0, exports.encodeContextBoolean)(buffer, 2, value.issueConfirmedNotifications);
    (0, exports.encodeContextUnsigned)(buffer, 3, value.timeRemaining);
    if (value.covIncrement > 0) {
        (0, exports.encodeContextReal)(buffer, 4, value.covIncrement);
    }
};
const bacappEncodeApplicationData = (buffer, value) => {
    if (value.value === null) {
        value.type = enum_1.ApplicationTag.NULL;
    }
    switch (value.type) {
        case enum_1.ApplicationTag.NULL:
            encodeApplicationNull(buffer);
            break;
        case enum_1.ApplicationTag.BOOLEAN:
            (0, exports.encodeApplicationBoolean)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.UNSIGNED_INTEGER:
            (0, exports.encodeApplicationUnsigned)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.SIGNED_INTEGER:
            (0, exports.encodeApplicationSigned)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.REAL:
            encodeApplicationReal(buffer, value.value);
            break;
        case enum_1.ApplicationTag.DOUBLE:
            encodeApplicationDouble(buffer, value.value);
            break;
        case enum_1.ApplicationTag.OCTET_STRING:
            (0, exports.encodeApplicationOctetString)(buffer, value.value, 0, value.value.length);
            break;
        case enum_1.ApplicationTag.CHARACTER_STRING:
            (0, exports.encodeApplicationCharacterString)(buffer, value.value, value.encoding);
            break;
        case enum_1.ApplicationTag.BIT_STRING:
            (0, exports.encodeApplicationBitstring)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.ENUMERATED:
            (0, exports.encodeApplicationEnumerated)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.DATE:
            (0, exports.encodeApplicationDate)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.TIME:
            (0, exports.encodeApplicationTime)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.TIMESTAMP:
            (0, exports.bacappEncodeTimestamp)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.DATETIME:
            bacappEncodeDatetime(buffer, value.value);
            break;
        case enum_1.ApplicationTag.OBJECTIDENTIFIER:
            (0, exports.encodeApplicationObjectId)(buffer, value.value.type, value.value.instance);
            break;
        case enum_1.ApplicationTag.COV_SUBSCRIPTION:
            encodeCovSubscription(buffer, value.value);
            break;
        case enum_1.ApplicationTag.READ_ACCESS_RESULT:
            (0, exports.encodeReadAccessResult)(buffer, value.value);
            break;
        case enum_1.ApplicationTag.READ_ACCESS_SPECIFICATION:
            (0, exports.encodeReadAccessSpecification)(buffer, value.value);
            break;
        case undefined:
            throw new Error('Cannot encode a value if the type has not been specified');
        default:
            throw new Error('Unknown type');
    }
};
exports.bacappEncodeApplicationData = bacappEncodeApplicationData;
const bacappEncodeDeviceObjPropertyRef = (buffer, value) => {
    (0, exports.encodeContextObjectId)(buffer, 0, value.objectId.type, value.objectId.instance);
    (0, exports.encodeContextEnumerated)(buffer, 1, value.id);
    if (value.arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
        (0, exports.encodeContextUnsigned)(buffer, 2, value.arrayIndex);
    }
    if (value.deviceIndentifier.type === enum_1.ObjectType.DEVICE) {
        (0, exports.encodeContextObjectId)(buffer, 3, value.deviceIndentifier.type, value.deviceIndentifier.instance);
    }
};
const bacappEncodeContextDeviceObjPropertyRef = (buffer, tagNumber, value) => {
    (0, exports.encodeOpeningTag)(buffer, tagNumber);
    bacappEncodeDeviceObjPropertyRef(buffer, value);
    (0, exports.encodeClosingTag)(buffer, tagNumber);
};
exports.bacappEncodeContextDeviceObjPropertyRef = bacappEncodeContextDeviceObjPropertyRef;
const bacappEncodePropertyState = (buffer, value) => {
    switch (value.type) {
        case enum_1.PropertyStates.BOOLEAN_VALUE:
            (0, exports.encodeContextBoolean)(buffer, 0, value.state === 1);
            break;
        case enum_1.PropertyStates.BINARY_VALUE:
            (0, exports.encodeContextEnumerated)(buffer, 1, value.state);
            break;
        case enum_1.PropertyStates.EVENT_TYPE:
            (0, exports.encodeContextEnumerated)(buffer, 2, value.state);
            break;
        case enum_1.PropertyStates.POLARITY:
            (0, exports.encodeContextEnumerated)(buffer, 3, value.state);
            break;
        case enum_1.PropertyStates.PROGRAM_CHANGE:
            (0, exports.encodeContextEnumerated)(buffer, 4, value.state);
            break;
        case enum_1.PropertyStates.PROGRAM_STATE:
            (0, exports.encodeContextEnumerated)(buffer, 5, value.state);
            break;
        case enum_1.PropertyStates.REASON_FOR_HALT:
            (0, exports.encodeContextEnumerated)(buffer, 6, value.state);
            break;
        case enum_1.PropertyStates.RELIABILITY:
            (0, exports.encodeContextEnumerated)(buffer, 7, value.state);
            break;
        case enum_1.PropertyStates.STATE:
            (0, exports.encodeContextEnumerated)(buffer, 8, value.state);
            break;
        case enum_1.PropertyStates.SYSTEM_STATUS:
            (0, exports.encodeContextEnumerated)(buffer, 9, value.state);
            break;
        case enum_1.PropertyStates.UNITS:
            (0, exports.encodeContextEnumerated)(buffer, 10, value.state);
            break;
        case enum_1.PropertyStates.UNSIGNED_VALUE:
            (0, exports.encodeContextUnsigned)(buffer, 11, value.state);
            break;
        case enum_1.PropertyStates.LIFE_SAFETY_MODE:
            (0, exports.encodeContextEnumerated)(buffer, 12, value.state);
            break;
        case enum_1.PropertyStates.LIFE_SAFETY_STATE:
            (0, exports.encodeContextEnumerated)(buffer, 13, value.state);
            break;
        default:
            break;
    }
};
exports.bacappEncodePropertyState = bacappEncodePropertyState;
const encodeContextBitstring = (buffer, tagNumber, bitString) => {
    const bitStringEncodedLength = bitstringBytesUsed(bitString) + 1;
    (0, exports.encodeTag)(buffer, tagNumber, true, bitStringEncodedLength);
    encodeBitstring(buffer, bitString);
};
exports.encodeContextBitstring = encodeContextBitstring;
const encodeContextSigned = (buffer, tagNumber, value) => {
    (0, exports.encodeTag)(buffer, tagNumber, true, getSignedLength(value));
    encodeBacnetSigned(buffer, value);
};
exports.encodeContextSigned = encodeContextSigned;
const encodeContextTime = (buffer, tagNumber, value) => {
    (0, exports.encodeTag)(buffer, tagNumber, true, 4);
    encodeBacnetTime(buffer, value);
};
const bacappEncodeContextDatetime = (buffer, tagNumber, value) => {
    if (value !== exports.ZERO_DATE) {
        (0, exports.encodeOpeningTag)(buffer, tagNumber);
        bacappEncodeDatetime(buffer, value);
        (0, exports.encodeClosingTag)(buffer, tagNumber);
    }
    else {
        throw new Error('wrong Datetime while bacapp encoding context');
    }
};
const decodeTagNumber = (buffer, offset) => {
    let len = 1;
    let tagNumber;
    if (isExtendedTagNumber(buffer[offset])) {
        tagNumber = buffer[offset + 1];
        len++;
    }
    else {
        tagNumber = buffer[offset] >> 4;
    }
    return {
        len,
        tagNumber,
    };
};
exports.decodeTagNumber = decodeTagNumber;
const decodeIsContextTag = (buffer, offset, tagNumber) => {
    const result = (0, exports.decodeTagNumber)(buffer, offset);
    return isContextSpecific(buffer[offset]) && result.tagNumber === tagNumber;
};
exports.decodeIsContextTag = decodeIsContextTag;
const decodeIsOpeningTagNumber = (buffer, offset, tagNumber) => {
    const result = (0, exports.decodeTagNumber)(buffer, offset);
    return isOpeningTag(buffer[offset]) && result.tagNumber === tagNumber;
};
exports.decodeIsOpeningTagNumber = decodeIsOpeningTagNumber;
const decodeIsClosingTagNumber = (buffer, offset, tagNumber) => {
    const result = (0, exports.decodeTagNumber)(buffer, offset);
    return isClosingTag(buffer[offset]) && result.tagNumber === tagNumber;
};
exports.decodeIsClosingTagNumber = decodeIsClosingTagNumber;
const decodeIsClosingTag = (buffer, offset) => {
    return (buffer[offset] & 0x07) === 7;
};
exports.decodeIsClosingTag = decodeIsClosingTag;
const decodeIsOpeningTag = (buffer, offset) => {
    return (buffer[offset] & 0x07) === 6;
};
exports.decodeIsOpeningTag = decodeIsOpeningTag;
const decodeObjectId = (buffer, offset) => {
    const result = (0, exports.decodeUnsigned)(buffer, offset, 4);
    const objectType = (result.value >> enum_1.ASN1_INSTANCE_BITS) & enum_1.ASN1_MAX_OBJECT;
    const instance = result.value & enum_1.ASN1_MAX_INSTANCE;
    return {
        len: result.len,
        objectType,
        instance,
    };
};
exports.decodeObjectId = decodeObjectId;
const decodeObjectIdSafe = (buffer, offset, lenValue) => {
    if (lenValue !== 4) {
        return {
            len: 0,
            objectType: 0,
            instance: 0,
        };
    }
    return (0, exports.decodeObjectId)(buffer, offset);
};
const decodeTagNumberAndValue = (buffer, offset) => {
    let value = 0;
    const tag = (0, exports.decodeTagNumber)(buffer, offset);
    let len = tag.len;
    if (isExtendedValue(buffer[offset])) {
        if (buffer[offset + len] === 255) {
            len++;
            const result = (0, exports.decodeUnsigned)(buffer, offset + len, 4);
            len += result.len;
            value = result.value;
        }
        else if (buffer[offset + len] === 254) {
            len++;
            const result = (0, exports.decodeUnsigned)(buffer, offset + len, 2);
            len += result.len;
            value = result.value;
        }
        else {
            value = buffer[offset + len];
            len++;
        }
    }
    else if (isOpeningTag(buffer[offset])) {
        value = 0;
    }
    else if (isClosingTag(buffer[offset])) {
        value = 0;
    }
    else {
        value = buffer[offset] & 0x07;
    }
    return {
        len,
        tagNumber: tag.tagNumber,
        value,
    };
};
exports.decodeTagNumberAndValue = decodeTagNumberAndValue;
const bacappDecodeApplicationData = (buffer, offset, maxOffset, objectType, propertyId) => {
    if (!isContextSpecific(buffer[offset])) {
        const tag = (0, exports.decodeTagNumberAndValue)(buffer, offset);
        if (tag) {
            const len = tag.len;
            const result = bacappDecodeData(buffer, offset + len, maxOffset, tag.tagNumber, tag.value);
            if (!result)
                return undefined;
            const resObj = {
                len: len + result.len,
                type: result.type,
                value: result.value,
            };
            if (result.encoding !== undefined)
                resObj.encoding = result.encoding;
            return resObj;
        }
    }
    else {
        return bacappDecodeContextApplicationData(buffer, offset, maxOffset, objectType, propertyId);
    }
    return undefined;
};
exports.bacappDecodeApplicationData = bacappDecodeApplicationData;
const encodeReadAccessResult = (buffer, value) => {
    (0, exports.encodeContextObjectId)(buffer, 0, value.objectId.type, value.objectId.instance);
    (0, exports.encodeOpeningTag)(buffer, 1);
    value.values.forEach((item) => {
        (0, exports.encodeContextEnumerated)(buffer, 2, item.property.id);
        if (item.property.index !== enum_1.ASN1_ARRAY_ALL) {
            (0, exports.encodeContextUnsigned)(buffer, 3, item.property.index);
        }
        if (item.value &&
            item.value[0] &&
            item.value[0].value &&
            item.value[0].value.type === 'BacnetError') {
            (0, exports.encodeOpeningTag)(buffer, 5);
            (0, exports.encodeApplicationEnumerated)(buffer, item.value[0].value.errorClass);
            (0, exports.encodeApplicationEnumerated)(buffer, item.value[0].value.errorCode);
            (0, exports.encodeClosingTag)(buffer, 5);
        }
        else {
            (0, exports.encodeOpeningTag)(buffer, 4);
            item.value.forEach((subItem) => (0, exports.bacappEncodeApplicationData)(buffer, subItem));
            (0, exports.encodeClosingTag)(buffer, 4);
        }
    });
    (0, exports.encodeClosingTag)(buffer, 1);
};
exports.encodeReadAccessResult = encodeReadAccessResult;
const decodeReadAccessResult = (buffer, offset, apduLen) => {
    let len = 0;
    const value = {
        objectId: { type: 0, instance: 0 },
        values: [],
    };
    if (!(0, exports.decodeIsContextTag)(buffer, offset + len, 0))
        return undefined;
    len++;
    const objectIdResult = (0, exports.decodeObjectId)(buffer, offset + len);
    value.objectId = {
        type: objectIdResult.objectType,
        instance: objectIdResult.instance,
    };
    len += objectIdResult.len;
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    const values = [];
    while (apduLen - len > 0) {
        const newEntry = {
            id: 0,
            index: enum_1.ASN1_ARRAY_ALL,
            value: [],
        };
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1)) {
            len++;
            break;
        }
        const tagResult1 = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tagResult1.len;
        if (tagResult1.tagNumber !== 2)
            return undefined;
        const enumResult = (0, exports.decodeEnumerated)(buffer, offset + len, tagResult1.value);
        newEntry.id = enumResult.value;
        len += enumResult.len;
        const tagResult2 = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        if (tagResult2.tagNumber === 3) {
            len += tagResult2.len;
            const unsignedResult = (0, exports.decodeUnsigned)(buffer, offset + len, tagResult2.value);
            newEntry.index = unsignedResult.value;
            len += unsignedResult.len;
        }
        const tagResult3 = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tagResult3.len;
        if (tagResult3.tagNumber === 4) {
            const localValues = [];
            while (len + offset <= buffer.length &&
                !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 4)) {
                const localResult = (0, exports.bacappDecodeApplicationData)(buffer, offset + len, apduLen + offset - 1, value.objectId.type, newEntry.id);
                if (!localResult)
                    return undefined;
                len += localResult.len;
                const resObj = {
                    value: localResult.value,
                    type: localResult.type,
                    len: localResult.len,
                    ...(localResult.encoding !== undefined && {
                        encoding: localResult.encoding,
                    }),
                };
                localValues.push(resObj);
            }
            if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 4))
                return undefined;
            if (localValues.length === 2 &&
                localValues[0].type === enum_1.ApplicationTag.DATE &&
                localValues[1].type === enum_1.ApplicationTag.TIME) {
                const date = localValues[0].value;
                const time = localValues[1].value;
                const bdatetime = new Date(date.getFullYear(), date.getMonth(), date.getDate(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds());
                newEntry.value = [
                    {
                        type: enum_1.ApplicationTag.DATETIME,
                        value: bdatetime,
                        len: localValues[1].len,
                    },
                ];
            }
            else {
                newEntry.value = localValues;
            }
            len++;
        }
        else if (tagResult3.tagNumber === 5) {
            const err = {
                errorClass: 0,
                errorCode: 0,
            };
            const errTagResult1 = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            len += errTagResult1.len;
            const errorClassResult = (0, exports.decodeEnumerated)(buffer, offset + len, errTagResult1.value);
            len += errorClassResult.len;
            err.errorClass = errorClassResult.value;
            const errTagResult2 = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            len += errTagResult2.len;
            const errorCodeResult = (0, exports.decodeEnumerated)(buffer, offset + len, errTagResult2.value);
            len += errorCodeResult.len;
            err.errorCode = errorCodeResult.value;
            if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 5))
                return undefined;
            len++;
            newEntry.value = [
                {
                    type: enum_1.ApplicationTag.ERROR,
                    value: err,
                    len: 0,
                },
            ];
        }
        values.push(newEntry);
    }
    value.values = values;
    return {
        len,
        value,
    };
};
exports.decodeReadAccessResult = decodeReadAccessResult;
const decodeSigned = (buffer, offset, length) => ({
    len: length,
    value: buffer.readIntBE(offset, length),
});
exports.decodeSigned = decodeSigned;
const decodeReal = (buffer, offset) => ({
    len: 4,
    value: buffer.readFloatBE(offset),
});
exports.decodeReal = decodeReal;
const decodeRealSafe = (buffer, offset, lenValue) => {
    if (lenValue !== 4) {
        return {
            len: lenValue,
            value: 0,
        };
    }
    return (0, exports.decodeReal)(buffer, offset);
};
const decodeDouble = (buffer, offset) => ({
    len: 8,
    value: buffer.readDoubleBE(offset),
});
const decodeDoubleSafe = (buffer, offset, lenValue) => {
    if (lenValue !== 8) {
        return {
            len: lenValue,
            value: 0,
        };
    }
    return decodeDouble(buffer, offset);
};
const decodeOctetString = (buffer, offset, maxLength, octetStringOffset, octetStringLength) => {
    const octetString = [];
    for (let i = octetStringOffset; i < octetStringOffset + octetStringLength; i++) {
        octetString.push(buffer[offset + i]);
    }
    return {
        len: octetStringLength,
        value: octetString,
    };
};
exports.decodeOctetString = decodeOctetString;
const multiCharsetCharacterstringDecode = (buffer, offset, maxLength, encoding, length) => {
    const stringBuf = Buffer.alloc(length);
    buffer.copy(stringBuf, 0, offset, offset + length);
    return {
        value: iconv.decode(stringBuf, getEncodingType(encoding, buffer, offset)),
        len: length + 1,
        encoding,
    };
};
const decodeCharacterString = (buffer, offset, maxLength, lenValue) => {
    return multiCharsetCharacterstringDecode(buffer, offset + 1, maxLength, buffer[offset], lenValue - 1);
};
exports.decodeCharacterString = decodeCharacterString;
const bitstringSetBitsUsed = (bitString, bytesUsed, unusedBits) => {
    bitString.bitsUsed = bytesUsed * 8;
    bitString.bitsUsed -= unusedBits;
};
const decodeBitstring = (buffer, offset, lenValue) => {
    let len = 0;
    const bitString = { value: [], bitsUsed: 0 };
    if (lenValue > 0) {
        const bytesUsed = lenValue - 1;
        if (bytesUsed <= enum_1.ASN1_MAX_BITSTRING_BYTES) {
            len = 1;
            for (let i = 0; i < bytesUsed; i++) {
                bitString.value.push(byteReverseBits(buffer[offset + len++]));
            }
            const unusedBits = buffer[offset] & 0x07;
            bitstringSetBitsUsed(bitString, bytesUsed, unusedBits);
        }
    }
    return {
        len,
        value: bitString,
    };
};
exports.decodeBitstring = decodeBitstring;
const isWildcardRawDate = (raw) => raw.year === 0xff &&
    raw.month === 0xff &&
    raw.day === 0xff &&
    raw.wday === 0xff;
const hasPartialWildcardRawDate = (raw) => !isWildcardRawDate(raw) &&
    (raw.year === 0xff ||
        raw.month === 0xff ||
        raw.day === 0xff ||
        raw.wday === 0xff);
const isInvalidConcreteRawDate = (raw) => raw.month < 1 || raw.month > 12 || raw.day < 1 || raw.day > 31;
const decodeDate = (buffer, offset) => {
    const raw = {
        year: buffer[offset],
        month: buffer[offset + 1],
        day: buffer[offset + 2],
        wday: buffer[offset + 3],
    };
    let date;
    if (isWildcardRawDate(raw) ||
        hasPartialWildcardRawDate(raw) ||
        isInvalidConcreteRawDate(raw)) {
        date = exports.ZERO_DATE;
    }
    else {
        const year = raw.year + 1900;
        const candidate = new Date(year, raw.month - 1, raw.day);
        const normalized = candidate.getFullYear() === year &&
            candidate.getMonth() === raw.month - 1 &&
            candidate.getDate() === raw.day;
        date = normalized ? candidate : exports.ZERO_DATE;
    }
    return {
        len: 4,
        value: date,
        raw,
    };
};
exports.decodeDate = decodeDate;
const decodeDateSafe = (buffer, offset, lenValue) => {
    if (lenValue !== 4) {
        return {
            len: lenValue,
            value: exports.ZERO_DATE,
        };
    }
    return (0, exports.decodeDate)(buffer, offset);
};
const decodeApplicationDate = (buffer, offset) => {
    const result = (0, exports.decodeTagNumber)(buffer, offset);
    if (result.tagNumber === enum_1.ApplicationTag.DATE) {
        const value = (0, exports.decodeDate)(buffer, offset + 1);
        return {
            len: value.len + 1,
            value: value.value,
        };
    }
    return undefined;
};
exports.decodeApplicationDate = decodeApplicationDate;
const decodeBacnetTime = (buffer, offset) => {
    const value = new Date(exports.ZERO_DATE);
    const hour = buffer[offset + 0];
    const min = buffer[offset + 1];
    const sec = buffer[offset + 2];
    let hundredths = buffer[offset + 3];
    if (hour !== 0xff || min !== 0xff || sec !== 0xff || hundredths !== 0xff) {
        if (hundredths > 100)
            hundredths = 0;
        value.setHours(hour);
        value.setMinutes(min);
        value.setSeconds(sec);
        value.setMilliseconds(hundredths * 10);
    }
    return {
        len: 4,
        value,
    };
};
exports.decodeBacnetTime = decodeBacnetTime;
const decodeBacnetTimeSafe = (buffer, offset, len) => {
    if (len !== 4) {
        return { len, value: exports.ZERO_DATE };
    }
    return (0, exports.decodeBacnetTime)(buffer, offset);
};
const decodeApplicationTime = (buffer, offset) => {
    const result = (0, exports.decodeTagNumber)(buffer, offset);
    if (result.tagNumber === enum_1.ApplicationTag.TIME) {
        const value = (0, exports.decodeBacnetTime)(buffer, offset + 1);
        return {
            len: value.len + 1,
            value: value.value,
        };
    }
    return undefined;
};
exports.decodeApplicationTime = decodeApplicationTime;
const decodeBacnetDatetime = (buffer, offset) => {
    let len = 0;
    const rawDate = (0, exports.decodeApplicationDate)(buffer, offset + len);
    if (!rawDate)
        return { len: 0, value: exports.ZERO_DATE };
    len += rawDate.len;
    const date = rawDate.value;
    const rawTime = (0, exports.decodeApplicationTime)(buffer, offset + len);
    if (!rawTime)
        return { len, value: date };
    len += rawTime.len;
    const time = rawTime.value;
    return {
        len,
        value: new Date(date.getFullYear(), date.getMonth(), date.getDay(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds()),
    };
};
const bacappDecodeData = (buffer, offset, maxLength, tagDataType, lenValueType) => {
    let result;
    const value = {
        len: 0,
        type: tagDataType,
        value: null,
    };
    switch (tagDataType) {
        case enum_1.ApplicationTag.NULL:
            value.value = null;
            break;
        case enum_1.ApplicationTag.BOOLEAN:
            value.value = lenValueType > 0;
            break;
        case enum_1.ApplicationTag.UNSIGNED_INTEGER:
            result = (0, exports.decodeUnsigned)(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.SIGNED_INTEGER:
            result = (0, exports.decodeSigned)(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.REAL:
            result = decodeRealSafe(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.DOUBLE:
            result = decodeDoubleSafe(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.OCTET_STRING:
            result = (0, exports.decodeOctetString)(buffer, offset, maxLength, 0, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.CHARACTER_STRING:
            result = (0, exports.decodeCharacterString)(buffer, offset, maxLength, lenValueType);
            value.len += result.len;
            value.value = result.value;
            value.encoding = result.encoding;
            break;
        case enum_1.ApplicationTag.BIT_STRING:
            result = (0, exports.decodeBitstring)(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.ENUMERATED:
            result = (0, exports.decodeEnumerated)(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.DATE:
            result = decodeDateSafe(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            if (result.raw) {
                value.raw = result.raw;
            }
            break;
        case enum_1.ApplicationTag.TIME:
            result = decodeBacnetTimeSafe(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.WEEKNDAY:
            result = decodeBacnetWeekNDaySafe(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = result.value;
            break;
        case enum_1.ApplicationTag.OBJECTIDENTIFIER:
            result = decodeObjectIdSafe(buffer, offset, lenValueType);
            value.len += result.len;
            value.value = { type: result.objectType, instance: result.instance };
            break;
        default:
            break;
    }
    return value;
};
const bacappContextTagType = (property, tagNumber) => {
    let tag = 0;
    switch (property) {
        case enum_1.PropertyIdentifier.ACTUAL_SHED_LEVEL:
        case enum_1.PropertyIdentifier.REQUESTED_SHED_LEVEL:
        case enum_1.PropertyIdentifier.EXPECTED_SHED_LEVEL:
            switch (tagNumber) {
                case 0:
                case 1:
                    tag = enum_1.ApplicationTag.UNSIGNED_INTEGER;
                    break;
                case 2:
                    tag = enum_1.ApplicationTag.REAL;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.ACTION:
            switch (tagNumber) {
                case 0:
                case 1:
                    tag = enum_1.ApplicationTag.OBJECTIDENTIFIER;
                    break;
                case 2:
                    tag = enum_1.ApplicationTag.ENUMERATED;
                    break;
                case 3:
                case 5:
                case 6:
                    tag = enum_1.ApplicationTag.UNSIGNED_INTEGER;
                    break;
                case 7:
                case 8:
                    tag = enum_1.ApplicationTag.BOOLEAN;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.LIST_OF_GROUP_MEMBERS:
            switch (tagNumber) {
                case 0:
                    tag = enum_1.ApplicationTag.OBJECTIDENTIFIER;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.EXCEPTION_SCHEDULE:
            switch (tagNumber) {
                case 1:
                    tag = enum_1.ApplicationTag.OBJECTIDENTIFIER;
                    break;
                case 3:
                    tag = enum_1.ApplicationTag.UNSIGNED_INTEGER;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.LOG_DEVICE_OBJECT_PROPERTY:
            switch (tagNumber) {
                case 0:
                case 3:
                    tag = enum_1.ApplicationTag.OBJECTIDENTIFIER;
                    break;
                case 1:
                    tag = enum_1.ApplicationTag.ENUMERATED;
                    break;
                case 2:
                    tag = enum_1.ApplicationTag.UNSIGNED_INTEGER;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.SUBORDINATE_LIST:
            switch (tagNumber) {
                case 0:
                case 1:
                    tag = enum_1.ApplicationTag.OBJECTIDENTIFIER;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.RECIPIENT_LIST:
            switch (tagNumber) {
                case 0:
                    tag = enum_1.ApplicationTag.OBJECTIDENTIFIER;
                    break;
                default:
                    break;
            }
            break;
        case enum_1.PropertyIdentifier.ACTIVE_COV_SUBSCRIPTIONS:
            switch (tagNumber) {
                case 0:
                case 1:
                    break;
                case 2:
                    tag = enum_1.ApplicationTag.BOOLEAN;
                    break;
                case 3:
                    tag = enum_1.ApplicationTag.UNSIGNED_INTEGER;
                    break;
                case 4:
                    tag = enum_1.ApplicationTag.REAL;
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
    return tag;
};
const decodeDeviceObjPropertyRef = (buffer, offset) => {
    let len = 0;
    if (!(0, exports.decodeIsContextTag)(buffer, offset + len, 0))
        return undefined;
    len++;
    let objectId = (0, exports.decodeObjectId)(buffer, offset + len);
    len += objectId.len;
    let result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== 1)
        return undefined;
    const id = (0, exports.decodeEnumerated)(buffer, offset + len, result.value);
    len += id.len;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    if (result.tagNumber === 2) {
        len += result.len;
        const unsignedResult = (0, exports.decodeUnsigned)(buffer, offset + len, result.value);
        len += unsignedResult.len;
    }
    if ((0, exports.decodeIsContextTag)(buffer, offset + len, 3)) {
        if (!isClosingTag(buffer[offset + len])) {
            len++;
            objectId = (0, exports.decodeObjectId)(buffer, offset + len);
            len += objectId.len;
        }
    }
    return {
        len,
        value: {
            objectId,
            id,
        },
    };
};
const decodeReadAccessSpecification = (buffer, offset, apduLen) => {
    let len = 0;
    const value = {
        objectId: { type: 0, instance: 0 },
        properties: [],
    };
    if (!(0, exports.decodeIsContextTag)(buffer, offset + len, 0))
        return undefined;
    len++;
    const objectIdResult = (0, exports.decodeObjectId)(buffer, offset + len);
    value.objectId = {
        type: objectIdResult.objectType,
        instance: objectIdResult.instance,
    };
    len += objectIdResult.len;
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    const propertyIdAndArrayIndex = [];
    while (apduLen - len > 1 &&
        !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1)) {
        const propertyRef = {
            id: 0,
            index: enum_1.ASN1_ARRAY_ALL,
        };
        if (!isContextSpecific(buffer[offset + len]))
            return undefined;
        const tagResult = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tagResult.len;
        if (tagResult.tagNumber !== 0)
            return undefined;
        if (len + tagResult.value >= apduLen)
            return undefined;
        const enumResult = (0, exports.decodeEnumerated)(buffer, offset + len, tagResult.value);
        propertyRef.id = enumResult.value;
        len += enumResult.len;
        if (isContextSpecific(buffer[offset + len]) &&
            !isClosingTag(buffer[offset + len])) {
            const indexTagResult = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            if (indexTagResult.tagNumber === 1) {
                len += indexTagResult.len;
                if (len + indexTagResult.value >= apduLen)
                    return undefined;
                const unsignedResult = (0, exports.decodeUnsigned)(buffer, offset + len, indexTagResult.value);
                propertyRef.index = unsignedResult.value;
                len += unsignedResult.len;
            }
        }
        propertyIdAndArrayIndex.push(propertyRef);
    }
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    value.properties = propertyIdAndArrayIndex;
    return {
        len,
        value,
    };
};
exports.decodeReadAccessSpecification = decodeReadAccessSpecification;
const decodeCovSubscription = (buffer, offset, apduLen) => {
    let len = 0;
    const value = {
        recipient: {
            network: 0,
            address: [],
        },
        subscriptionProcessId: 0,
        monitoredObjectId: { type: 0, instance: 0 },
        monitoredProperty: { id: 0, index: 0 },
        issueConfirmedNotifications: false,
        timeRemaining: 0,
        covIncrement: 0,
    };
    let result;
    let decodedValue;
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 0))
        return undefined;
    len++;
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 0))
        return undefined;
    len++;
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== enum_1.ApplicationTag.UNSIGNED_INTEGER)
        return undefined;
    decodedValue = (0, exports.decodeUnsigned)(buffer, offset + len, result.value);
    len += decodedValue.len;
    value.recipient.network = decodedValue.value;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== enum_1.ApplicationTag.OCTET_STRING)
        return undefined;
    decodedValue = (0, exports.decodeOctetString)(buffer, offset + len, apduLen, 0, result.value);
    len += decodedValue.len;
    value.recipient.address = decodedValue.value;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0))
        return undefined;
    len++;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== 1)
        return undefined;
    decodedValue = (0, exports.decodeUnsigned)(buffer, offset + len, result.value);
    len += decodedValue.len;
    value.subscriptionProcessId = decodedValue.value;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0))
        return undefined;
    len++;
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== 0)
        return undefined;
    decodedValue = (0, exports.decodeObjectId)(buffer, offset + len);
    len += decodedValue.len;
    value.monitoredObjectId = {
        type: decodedValue.objectType,
        instance: decodedValue.instance,
    };
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== 1)
        return undefined;
    decodedValue = (0, exports.decodeEnumerated)(buffer, offset + len, result.value);
    len += decodedValue.len;
    value.monitoredProperty.id = decodedValue.value;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    if (result.tagNumber === 2) {
        len += result.len;
        decodedValue = (0, exports.decodeUnsigned)(buffer, offset + len, result.value);
        len += decodedValue.len;
        value.monitoredProperty.index = decodedValue.value;
    }
    else {
        value.monitoredProperty.index = enum_1.ASN1_ARRAY_ALL;
    }
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1))
        return undefined;
    len++;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== 2)
        return undefined;
    value.issueConfirmedNotifications = buffer[offset + len] > 0;
    len++;
    result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    if (result.tagNumber !== 3)
        return undefined;
    decodedValue = (0, exports.decodeUnsigned)(buffer, offset + len, result.value);
    len += decodedValue.len;
    value.timeRemaining = decodedValue.value;
    if (len < apduLen && !isClosingTag(buffer[offset + len])) {
        result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== 4)
            return undefined;
        decodedValue = (0, exports.decodeReal)(buffer, offset + len);
        len += decodedValue.len;
        value.covIncrement = decodedValue.value;
    }
    return {
        len,
        value,
    };
};
const decodeCalendarDate = (buffer, offset) => ({
    len: 4,
    year: buffer[offset],
    month: buffer[offset + 1],
    day: buffer[offset + 2],
    wday: buffer[offset + 3],
});
const decodeCalendarDateRange = (buffer, offset) => {
    let len = 1;
    const startDate = (0, exports.decodeDate)(buffer, offset + len);
    len += startDate.len + 1;
    const endDate = (0, exports.decodeDate)(buffer, offset + len);
    len += endDate.len + 1;
    return {
        len,
        startDate,
        endDate,
    };
};
const decodeCalendarWeekDay = (buffer, offset) => ({
    len: 3,
    month: buffer[offset],
    week: buffer[offset + 1],
    wday: buffer[offset + 2],
});
const decodeBacnetWeekNDay = (buffer, offset) => ({
    len: 3,
    value: {
        month: buffer[offset],
        week: buffer[offset + 1],
        wday: buffer[offset + 2],
    },
});
const decodeBacnetWeekNDaySafe = (buffer, offset, len) => {
    if (len !== 3) {
        return {
            len,
            value: {
                month: 0xff,
                week: 0xff,
                wday: 0xff,
            },
        };
    }
    return decodeBacnetWeekNDay(buffer, offset);
};
const decodeCalendar = (buffer, offset, apduLen) => {
    let len = 0;
    const entries = [];
    while (len < apduLen) {
        const result = (0, exports.decodeTagNumber)(buffer, offset + len);
        len += result.len;
        switch (result.tagNumber) {
            case 0: {
                const decodedValue = decodeCalendarDate(buffer, offset + len);
                len += decodedValue.len;
                entries.push(decodedValue);
                break;
            }
            case 1: {
                const decodedValue = decodeCalendarDateRange(buffer, offset + len);
                len += decodedValue.len;
                entries.push(decodedValue);
                break;
            }
            case 2: {
                const decodedValue = decodeCalendarWeekDay(buffer, offset + len);
                len += decodedValue.len;
                entries.push(decodedValue);
                break;
            }
            default:
                return {
                    len: len - 1,
                    value: entries,
                };
        }
    }
    return undefined;
};
const decodeWeeklySchedule = (buffer, offset, apduLen) => {
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset, 0))
        return undefined;
    let len = 1;
    let day = 0;
    const result = {
        0: [],
        1: [],
        2: [],
        3: [],
        4: [],
        5: [],
        6: [],
    };
    while (len < apduLen &&
        !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3)) {
        if (day > 6)
            return undefined;
        if ((0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 0)) {
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
        }
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0)) {
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            day += 1;
            continue;
        }
        const decoded = {
            time: null,
            value: null,
        };
        let tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        let value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
        if (!value)
            return undefined;
        decoded.time = value;
        len += value.len;
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
        if (!value)
            return undefined;
        len += value.len;
        decoded.value = value;
        result[day].push(decoded);
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0)) {
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            day += 1;
        }
    }
    if (len >= apduLen)
        return undefined;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3))
        return undefined;
    len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
    return { len, value: Object.values(result) };
};
exports.decodeWeeklySchedule = decodeWeeklySchedule;
const decodeExceptionSchedule = (buffer, offset, apduLen) => {
    let len = 0;
    if ((0, exports.decodeIsClosingTagNumber)(buffer, offset, 3)) {
        len += (0, exports.decodeTagNumberAndValue)(buffer, offset).len;
        return { len, value: [] };
    }
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset, 0))
        return undefined;
    len++;
    const result = [];
    while (len < apduLen &&
        !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3)) {
        if ((0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 0)) {
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
        }
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0)) {
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            continue;
        }
        const decoded = {
            date: null,
            events: [],
            priority: null,
        };
        let tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        if (tag.tagNumber === 1 ||
            (0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1)) {
            if ((0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1)) {
                len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            }
            const dates = [];
            while (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1)) {
                tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
                len += tag.len;
                const value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
                if (!value)
                    return undefined;
                len += value.len;
                dates.push(value);
            }
            tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            len += tag.len;
            decoded.date = {
                len: 8,
                type: enum_1.ApplicationTag.DATERANGE,
                value: dates,
            };
        }
        else if (tag.tagNumber === 0 && tag.value === 4) {
            const value = bacappDecodeData(buffer, offset + len, apduLen + offset, enum_1.ApplicationTag.DATE, tag.value);
            if (!value)
                return undefined;
            decoded.date = value;
            len += value.len;
        }
        else if (tag.tagNumber === 2 && tag.value === 3) {
            const value = bacappDecodeData(buffer, offset + len, apduLen + offset, enum_1.ApplicationTag.WEEKNDAY, tag.value);
            if (!value)
                return undefined;
            decoded.date = value;
            len += value.len;
        }
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0) &&
            (0, exports.decodeIsOpeningTagNumber)(buffer, offset + len + 1, 2)) {
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            while (len < apduLen &&
                !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 2)) {
                const event = {
                    time: null,
                    value: null,
                };
                tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
                len += tag.len;
                let value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
                if (!value)
                    return undefined;
                len += value.len;
                event.time = value;
                tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
                len += tag.len;
                value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
                if (!value)
                    return undefined;
                event.value = value;
                len += value.len;
                decoded.events.push(event);
                if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 2)) {
                    len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
                    break;
                }
            }
        }
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3)) {
            len += tag.len;
            return { len, value: result };
        }
        len += tag.len;
        const priority = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
        if (!priority)
            return undefined;
        decoded.priority = priority;
        len += priority.len;
        result.push(decoded);
    }
    if (len >= apduLen)
        return undefined;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3))
        return undefined;
    len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
    return { len, value: result };
};
exports.decodeExceptionSchedule = decodeExceptionSchedule;
const decodeScheduleEffectivePeriod = (buffer, offset, apduLen) => {
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset - 1, 3))
        return undefined;
    let len = 0;
    const result = [];
    while (len < apduLen &&
        !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3)) {
        const tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        const value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
        if (!value)
            return undefined;
        result.push(value);
        len += value.len;
    }
    if (len >= apduLen)
        return undefined;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3))
        return undefined;
    len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
    return { len, value: result };
};
exports.decodeScheduleEffectivePeriod = decodeScheduleEffectivePeriod;
const decodeCalendarDatelist = (buffer, offset, apduLen) => {
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset - 1, 3))
        return undefined;
    let len = 0;
    const result = [];
    while (len < apduLen &&
        !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3)) {
        let tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        if (tag.tagNumber === 1 ||
            (0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1)) {
            if ((0, exports.decodeIsOpeningTagNumber)(buffer, offset + len, 1)) {
                len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
            }
            const dates = [];
            while (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 1)) {
                tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
                len += tag.len;
                const value = bacappDecodeData(buffer, offset + len, apduLen + offset, tag.tagNumber, tag.value);
                if (!value)
                    return undefined;
                len += value.len;
                dates.push(value);
            }
            tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            len += tag.len;
            result.push({
                len: 8,
                type: enum_1.ApplicationTag.DATERANGE,
                value: dates,
            });
        }
        else if (tag.tagNumber === 0 && tag.value === 4) {
            const value = bacappDecodeData(buffer, offset + len, apduLen + offset, enum_1.ApplicationTag.DATE, tag.value);
            if (!value)
                return undefined;
            result.push(value);
            len += value.len;
        }
        else if (tag.tagNumber === 2 && tag.value === 3) {
            const value = bacappDecodeData(buffer, offset + len, apduLen + offset, enum_1.ApplicationTag.WEEKNDAY, tag.value);
            if (!value)
                return undefined;
            result.push(value);
            len += value.len;
        }
    }
    if (len >= apduLen)
        return undefined;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 3))
        return undefined;
    len += (0, exports.decodeTagNumberAndValue)(buffer, offset + len).len;
    return { len, value: result };
};
exports.decodeCalendarDatelist = decodeCalendarDatelist;
const decodeRange = (buffer, offset, maxOffset) => {
    if (!(0, exports.decodeIsOpeningTagNumber)(buffer, offset, 0))
        return undefined;
    let len = 0;
    const result = [];
    while (offset + len < maxOffset &&
        !(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 5)) {
        let tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        const date = bacappDecodeData(buffer, offset + len, maxOffset, tag.tagNumber, tag.value);
        if (!date)
            return undefined;
        len += date.len;
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        const time = bacappDecodeData(buffer, offset + len, maxOffset, tag.tagNumber, tag.value);
        if (!time)
            return undefined;
        len += time.len;
        if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 0) ||
            !(0, exports.decodeIsOpeningTagNumber)(buffer, offset + len + 1, 1)) {
            return undefined;
        }
        len += 2;
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        let value;
        if (tag.tagNumber === 2 && tag.value === 4) {
            value = bacappDecodeData(buffer, offset + len, maxOffset, enum_1.ApplicationTag.REAL, tag.value);
        }
        else if (tag.tagNumber === 4 && tag.value === 1) {
            value = bacappDecodeData(buffer, offset + len, maxOffset, enum_1.ApplicationTag.ENUMERATED, tag.value);
        }
        else if (tag.tagNumber === 3 && tag.value === 1) {
            value = bacappDecodeData(buffer, offset + len, maxOffset, enum_1.ApplicationTag.ENUMERATED, tag.value);
        }
        if (!value)
            return undefined;
        len += value.len;
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        tag = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        len += tag.len;
        const status = bacappDecodeData(buffer, offset + len, maxOffset, enum_1.ApplicationTag.BIT_STRING, tag.value);
        if (!status)
            return undefined;
        len += status.len;
        const d = date.value;
        const t = time.value;
        const statusBits = status.value;
        const timestamp = new Date(d.getFullYear(), d.getMonth(), d.getDate(), t.getHours(), t.getMinutes(), t.getSeconds(), t.getMilliseconds());
        result.push({
            timestamp,
            value: value.value,
            status: {
                out_of_service: ((statusBits.value[0] >> 0) & 1) !== 0,
                overridden: ((statusBits.value[0] >> 1) & 1) !== 0,
                fault: ((statusBits.value[0] >> 2) & 1) !== 0,
                in_alarm: ((statusBits.value[0] >> 3) & 1) !== 0,
            },
        });
    }
    if (offset + len >= maxOffset)
        return undefined;
    if (!(0, exports.decodeIsClosingTagNumber)(buffer, offset + len, 5))
        return undefined;
    return { len, value: result };
};
exports.decodeRange = decodeRange;
const bacappDecodeContextApplicationData = (buffer, offset, maxOffset, objectType, propertyId) => {
    let len = 0;
    if (isContextSpecific(buffer[offset])) {
        if (propertyId === enum_1.PropertyIdentifier.LIST_OF_GROUP_MEMBERS) {
            const result = (0, exports.decodeReadAccessSpecification)(buffer, offset, maxOffset);
            if (!result)
                return undefined;
            return {
                type: enum_1.ApplicationTag.READ_ACCESS_SPECIFICATION,
                value: result.value,
                len: result.len,
            };
        }
        if (propertyId === enum_1.PropertyIdentifier.ACTIVE_COV_SUBSCRIPTIONS) {
            const result = decodeCovSubscription(buffer, offset, maxOffset);
            if (!result)
                return undefined;
            return {
                type: enum_1.ApplicationTag.COV_SUBSCRIPTION,
                value: result.value,
                len: result.len,
            };
        }
        if (objectType === enum_1.ObjectType.GROUP &&
            propertyId === enum_1.PropertyIdentifier.PRESENT_VALUE) {
            const result = (0, exports.decodeReadAccessResult)(buffer, offset, maxOffset);
            if (!result)
                return undefined;
            return {
                type: enum_1.ApplicationTag.READ_ACCESS_RESULT,
                value: result.value,
                len: result.len,
            };
        }
        if (propertyId ===
            enum_1.PropertyIdentifier.LIST_OF_OBJECT_PROPERTY_REFERENCES ||
            propertyId === enum_1.PropertyIdentifier.LOG_DEVICE_OBJECT_PROPERTY ||
            propertyId === enum_1.PropertyIdentifier.OBJECT_PROPERTY_REFERENCE) {
            const result = decodeDeviceObjPropertyRef(buffer, offset);
            if (!result)
                return undefined;
            return {
                type: enum_1.ApplicationTag.OBJECT_PROPERTY_REFERENCE,
                value: result.value,
                len: result.len,
            };
        }
        if (propertyId === enum_1.PropertyIdentifier.DATE_LIST) {
            const result = decodeCalendar(buffer, offset, maxOffset);
            if (!result)
                return undefined;
            return {
                type: enum_1.ApplicationTag.CONTEXT_SPECIFIC_DECODED,
                value: result.value,
                len: result.len,
            };
        }
        if (propertyId === enum_1.PropertyIdentifier.EVENT_TIME_STAMPS) {
            let subEvtResult;
            const evtResult = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            len += 1;
            if (evtResult.tagNumber === 0) {
                subEvtResult = (0, exports.decodeBacnetTime)(buffer, offset + 1);
                return {
                    type: enum_1.ApplicationTag.TIMESTAMP,
                    value: subEvtResult.value,
                    len: subEvtResult.len + 1,
                };
            }
            if (evtResult.tagNumber === 1) {
                subEvtResult = (0, exports.decodeUnsigned)(buffer, offset + len, evtResult.value);
                return {
                    type: enum_1.ApplicationTag.UNSIGNED_INTEGER,
                    value: subEvtResult.value,
                    len: subEvtResult.len + 1,
                };
            }
            if (evtResult.tagNumber === 2) {
                subEvtResult = decodeBacnetDatetime(buffer, offset + len);
                return {
                    type: enum_1.ApplicationTag.TIMESTAMP,
                    value: subEvtResult.value,
                    len: subEvtResult.len + 2,
                };
            }
            return undefined;
        }
        const list = [];
        const tagResult = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
        const multipleValues = isOpeningTag(buffer[offset + len]);
        while (len + offset <= maxOffset &&
            !isClosingTag(buffer[offset + len])) {
            const subResult = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
            if (!subResult)
                return undefined;
            if (subResult.value === 0) {
                len += subResult.len;
                const result = (0, exports.bacappDecodeApplicationData)(buffer, offset + len, maxOffset, enum_1.ASN1_MAX_OBJECT_TYPE, enum_1.ASN1_MAX_PROPERTY_ID);
                if (!result)
                    return undefined;
                list.push(result);
                len += result.len;
            }
            else {
                const overrideTagNumber = bacappContextTagType(propertyId, subResult.tagNumber);
                if (overrideTagNumber !== enum_1.ASN1_MAX_APPLICATION_TAG) {
                    subResult.tagNumber = overrideTagNumber;
                }
                const bacappResult = bacappDecodeData(buffer, offset + len + subResult.len, maxOffset, subResult.tagNumber, subResult.value);
                if (!bacappResult)
                    return undefined;
                if (bacappResult.len === subResult.value) {
                    const resObj = {
                        value: bacappResult.value,
                        type: bacappResult.type,
                    };
                    if (bacappResult.encoding !== undefined)
                        resObj.encoding = bacappResult.encoding;
                    list.push(resObj);
                    len += subResult.len + subResult.value;
                }
                else {
                    list.push({
                        value: buffer.slice(offset + len + subResult.len, offset + len + subResult.len + subResult.value),
                        type: enum_1.ApplicationTag.CONTEXT_SPECIFIC_ENCODED,
                    });
                    len += subResult.len + subResult.value;
                }
            }
            if (multipleValues === false) {
                return {
                    len,
                    value: list[0],
                    type: enum_1.ApplicationTag.CONTEXT_SPECIFIC_DECODED,
                };
            }
        }
        if (len + offset > maxOffset)
            return undefined;
        if ((0, exports.decodeIsClosingTagNumber)(buffer, offset + len, tagResult.tagNumber)) {
            len++;
        }
        return {
            len,
            value: list,
            type: enum_1.ApplicationTag.CONTEXT_SPECIFIC_DECODED,
        };
    }
    return undefined;
};
const bacappEncodeTimestamp = (buffer, value) => {
    switch (value.type) {
        case enum_1.TimeStamp.TIME:
            encodeContextTime(buffer, 0, value.value);
            break;
        case enum_1.TimeStamp.SEQUENCE_NUMBER:
            (0, exports.encodeContextUnsigned)(buffer, 1, value.value);
            break;
        case enum_1.TimeStamp.DATETIME:
            bacappEncodeContextDatetime(buffer, 2, value.value);
            break;
        default:
            throw new Error('NOT_IMPLEMENTED');
    }
};
exports.bacappEncodeTimestamp = bacappEncodeTimestamp;
const bacappEncodeContextTimestamp = (buffer, tagNumber, value) => {
    (0, exports.encodeOpeningTag)(buffer, tagNumber);
    (0, exports.bacappEncodeTimestamp)(buffer, value);
    (0, exports.encodeClosingTag)(buffer, tagNumber);
};
exports.bacappEncodeContextTimestamp = bacappEncodeContextTimestamp;
const decodeContextCharacterString = (buffer, offset, maxLength, tagNumber) => {
    let len = 0;
    if (!(0, exports.decodeIsContextTag)(buffer, offset + len, tagNumber))
        return undefined;
    const result = (0, exports.decodeTagNumberAndValue)(buffer, offset + len);
    len += result.len;
    const decodedValue = multiCharsetCharacterstringDecode(buffer, offset + 1 + len, maxLength, buffer[offset + len], result.value - 1);
    if (!decodedValue)
        return undefined;
    len += result.value;
    return {
        len,
        value: decodedValue.value,
        encoding: decodedValue.encoding,
    };
};
exports.decodeContextCharacterString = decodeContextCharacterString;
const decodeIsContextTagWithLength = (buffer, offset, tagNumber) => {
    const result = (0, exports.decodeTagNumber)(buffer, offset);
    return {
        len: result.len,
        value: isContextSpecific(buffer[offset]) && result.tagNumber === tagNumber,
    };
};
const decodeContextObjectId = (buffer, offset, tagNumber) => {
    const result = decodeIsContextTagWithLength(buffer, offset, tagNumber);
    if (!result.value)
        return undefined;
    const decodedValue = (0, exports.decodeObjectId)(buffer, offset + result.len);
    decodedValue.len += result.len;
    return decodedValue;
};
exports.decodeContextObjectId = decodeContextObjectId;
const encodeBacnetCharacterString = (buffer, value, encoding) => {
    encoding = encoding || enum_1.CharacterStringEncoding.UTF_8;
    buffer.buffer[buffer.offset++] = encoding;
    const bufEncoded = iconv.encode(value, getEncodingType(encoding));
    buffer.offset += bufEncoded.copy(buffer.buffer, buffer.offset);
};
const encodeApplicationCharacterString = (buffer, value, encoding) => {
    const tmp = getBuffer();
    encodeBacnetCharacterString(tmp, value, encoding);
    (0, exports.encodeTag)(buffer, enum_1.ApplicationTag.CHARACTER_STRING, false, tmp.offset);
    tmp.buffer.copy(buffer.buffer, buffer.offset, 0, tmp.offset);
    buffer.offset += tmp.offset;
};
exports.encodeApplicationCharacterString = encodeApplicationCharacterString;
const encodeContextCharacterString = (buffer, tagNumber, value, encoding) => {
    const tmp = getBuffer();
    encodeBacnetCharacterString(tmp, value, encoding);
    (0, exports.encodeTag)(buffer, tagNumber, true, tmp.offset);
    tmp.buffer.copy(buffer.buffer, buffer.offset, 0, tmp.offset);
    buffer.offset += tmp.offset;
};
exports.encodeContextCharacterString = encodeContextCharacterString;
//# sourceMappingURL=asn1.js.map