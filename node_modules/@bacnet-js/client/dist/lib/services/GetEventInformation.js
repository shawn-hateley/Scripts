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
class GetEventInformation extends AbstractServices_1.BacnetAckService {
    static encode(buffer, lastReceivedObjectId) {
        baAsn1.encodeContextObjectId(buffer, 0, lastReceivedObjectId.type, lastReceivedObjectId.instance);
    }
    static decode(buffer, offset) {
        let len = 0;
        const result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        let lastReceivedObjectId = null;
        if (offset + len < buffer.length) {
            const decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
            len += decodedValue.len;
            lastReceivedObjectId = {
                type: decodedValue.objectType,
                instance: decodedValue.instance,
            };
        }
        return { len, lastReceivedObjectId };
    }
    static encodeAcknowledge(buffer, events, moreEvents) {
        baAsn1.encodeOpeningTag(buffer, 0);
        events.forEach((eventData) => {
            baAsn1.encodeContextObjectId(buffer, 0, eventData.objectId.type, eventData.objectId.instance);
            baAsn1.encodeContextEnumerated(buffer, 1, eventData.eventState);
            baAsn1.encodeContextBitstring(buffer, 2, eventData.acknowledgedTransitions);
            baAsn1.encodeOpeningTag(buffer, 3);
            for (let i = 0; i < 3; i++) {
                baAsn1.bacappEncodeTimestamp(buffer, eventData.eventTimeStamps[i]);
            }
            baAsn1.encodeClosingTag(buffer, 3);
            baAsn1.encodeContextEnumerated(buffer, 4, eventData.notifyType);
            baAsn1.encodeContextBitstring(buffer, 5, eventData.eventEnable);
            baAsn1.encodeOpeningTag(buffer, 6);
            for (let i = 0; i < 3; i++) {
                baAsn1.encodeApplicationUnsigned(buffer, eventData.eventPriorities[i]);
            }
            baAsn1.encodeClosingTag(buffer, 6);
        });
        baAsn1.encodeClosingTag(buffer, 0);
        baAsn1.encodeContextBoolean(buffer, 1, moreEvents);
    }
    static decodeAcknowledge(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        const value = {};
        if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 0))
            return undefined;
        len++;
        value.events = [];
        while (apduLen - len > 3) {
            const event = {};
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
            len += decodedValue.len;
            event.objectId = {
                type: decodedValue.objectType,
                instance: decodedValue.instance,
            };
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
            len += decodedValue.len;
            event.eventState = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeBitstring(buffer, offset + len, result.value);
            len += decodedValue.len;
            event.acknowledgedTransitions = decodedValue.value;
            if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 3))
                return undefined;
            len++;
            event.eventTimeStamps = [];
            for (let i = 0; i < 3; i++) {
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                if (result.tagNumber === enum_1.TimeStamp.TIME) {
                    decodedValue = baAsn1.decodeBacnetTime(buffer, offset + len);
                    len += decodedValue.len;
                    event.eventTimeStamps[i] = {
                        value: decodedValue.value,
                        type: enum_1.TimeStamp.TIME,
                    };
                }
                else if (result.tagNumber === enum_1.TimeStamp.SEQUENCE_NUMBER) {
                    decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                    len += decodedValue.len;
                    event.eventTimeStamps[i] = {
                        value: decodedValue.value,
                        type: enum_1.TimeStamp.SEQUENCE_NUMBER,
                    };
                }
                else if (result.tagNumber === enum_1.TimeStamp.DATETIME) {
                    const dateRaw = baAsn1.decodeApplicationDate(buffer, offset + len);
                    len += dateRaw.len;
                    const date = dateRaw.value;
                    const timeRaw = baAsn1.decodeApplicationTime(buffer, offset + len);
                    len += timeRaw.len;
                    const time = timeRaw.value;
                    event.eventTimeStamps[i] = {
                        value: new Date(date.getFullYear(), date.getMonth(), date.getDate(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds()),
                        type: enum_1.TimeStamp.DATETIME,
                    };
                    len++;
                }
            }
            if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 3))
                return undefined;
            len++;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
            len += decodedValue.len;
            event.notifyType = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeBitstring(buffer, offset + len, result.value);
            len += decodedValue.len;
            event.eventEnable = decodedValue.value;
            if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 6))
                return undefined;
            len++;
            event.eventPriorities = [];
            for (let i = 0; i < 3; i++) {
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                event.eventPriorities[i] = decodedValue.value;
            }
            if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 6))
                return undefined;
            len++;
            value.events.push(event);
        }
        if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 0))
            return undefined;
        len++;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        value.moreEvents = buffer[offset + len] > 0;
        len++;
        value.len = len;
        return value;
    }
}
exports.default = GetEventInformation;
//# sourceMappingURL=GetEventInformation.js.map