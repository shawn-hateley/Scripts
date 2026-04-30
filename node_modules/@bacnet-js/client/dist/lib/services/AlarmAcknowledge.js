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
const baAsn1 = __importStar(require("../asn1"));
const enum_1 = require("../enum");
const AbstractServices_1 = require("./AbstractServices");
class AlarmAcknowledge extends AbstractServices_1.BacnetService {
    static encode(buffer, ackProcessId, eventObjectId, eventStateAcknowledged, ackSource, eventTimeStamp, ackTimeStamp) {
        baAsn1.encodeContextUnsigned(buffer, 0, ackProcessId);
        baAsn1.encodeContextObjectId(buffer, 1, eventObjectId.type, eventObjectId.instance);
        baAsn1.encodeContextEnumerated(buffer, 2, eventStateAcknowledged);
        baAsn1.bacappEncodeContextTimestamp(buffer, 3, eventTimeStamp);
        baAsn1.encodeContextCharacterString(buffer, 4, ackSource);
        baAsn1.bacappEncodeContextTimestamp(buffer, 5, ackTimeStamp);
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        const value = {
            len: 0,
            acknowledgedProcessId: 0,
            eventObjectId: { type: 0, instance: 0 },
            eventStateAcknowledged: 0,
            eventTimeStamp: new Date(),
            acknowledgeSource: '',
            acknowledgeTimeStamp: new Date(),
        };
        let result;
        let decodedValue;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        value.acknowledgedProcessId = decodedValue.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        value.eventObjectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        value.eventStateAcknowledged = decodedValue.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber === enum_1.TimeStamp.TIME) {
            decodedValue = baAsn1.decodeBacnetTime(buffer, offset + len);
            len += decodedValue.len;
            value.eventTimeStamp = decodedValue.value;
        }
        else if (result.tagNumber === enum_1.TimeStamp.SEQUENCE_NUMBER) {
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.eventTimeStamp = decodedValue.value;
        }
        else if (result.tagNumber === enum_1.TimeStamp.DATETIME) {
            const dateRaw = baAsn1.decodeApplicationDate(buffer, offset + len);
            len += dateRaw.len;
            const date = dateRaw.value;
            const timeRaw = baAsn1.decodeApplicationTime(buffer, offset + len);
            len += timeRaw.len;
            const time = timeRaw.value;
            value.eventTimeStamp = new Date(date.getFullYear(), date.getMonth(), date.getDate(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds());
            len++;
        }
        len++;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeCharacterString(buffer, offset + len, apduLen - (offset + len), result.value);
        value.acknowledgeSource = decodedValue.value;
        len += decodedValue.len;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber === enum_1.TimeStamp.TIME) {
            decodedValue = baAsn1.decodeBacnetTime(buffer, offset + len);
            len += decodedValue.len;
            value.acknowledgeTimeStamp = decodedValue.value;
        }
        else if (result.tagNumber === enum_1.TimeStamp.SEQUENCE_NUMBER) {
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.acknowledgeTimeStamp = decodedValue.value;
        }
        else if (result.tagNumber === enum_1.TimeStamp.DATETIME) {
            const dateRaw = baAsn1.decodeApplicationDate(buffer, offset + len);
            len += dateRaw.len;
            const date = dateRaw.value;
            const timeRaw = baAsn1.decodeApplicationTime(buffer, offset + len);
            len += timeRaw.len;
            const time = timeRaw.value;
            value.acknowledgeTimeStamp = new Date(date.getFullYear(), date.getMonth(), date.getDate(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds());
            len++;
        }
        len++;
        value.len = len;
        return value;
    }
}
exports.default = AlarmAcknowledge;
//# sourceMappingURL=AlarmAcknowledge.js.map