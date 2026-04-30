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
const AbstractServices_1 = require("./AbstractServices");
class EventInformation extends AbstractServices_1.BacnetService {
    static encode(buffer, events, moreEvents) {
        baAsn1.encodeOpeningTag(buffer, 0);
        events.forEach((event) => {
            baAsn1.encodeContextObjectId(buffer, 0, event.objectId.type, event.objectId.instance);
            baAsn1.encodeContextEnumerated(buffer, 1, event.eventState);
            baAsn1.encodeContextBitstring(buffer, 2, event.acknowledgedTransitions);
            baAsn1.encodeOpeningTag(buffer, 3);
            for (let i = 0; i < 3; i++) {
                baAsn1.encodeApplicationDate(buffer, event.eventTimeStamps[i]);
                baAsn1.encodeApplicationTime(buffer, event.eventTimeStamps[i]);
            }
            baAsn1.encodeClosingTag(buffer, 3);
            baAsn1.encodeContextEnumerated(buffer, 4, event.notifyType);
            baAsn1.encodeContextBitstring(buffer, 5, event.eventEnable);
            baAsn1.encodeOpeningTag(buffer, 6);
            for (let i = 0; i < 3; i++) {
                baAsn1.encodeApplicationUnsigned(buffer, event.eventPriorities[i]);
            }
            baAsn1.encodeClosingTag(buffer, 6);
        });
        baAsn1.encodeClosingTag(buffer, 0);
        baAsn1.encodeContextBoolean(buffer, 1, moreEvents);
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        len++;
        const alarms = [];
        while (len < apduLen &&
            !baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 0)) {
            const value = {};
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
            len += decodedValue.len;
            value.objectId = {
                type: decodedValue.objectType,
                instance: decodedValue.instance,
            };
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.eventState = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeBitstring(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.acknowledgedTransitions = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            value.eventTimeStamps = [];
            for (let i = 0; i < 3; i++) {
                decodedValue = baAsn1.decodeApplicationDate(buffer, offset + len);
                len += decodedValue.len;
                const date = decodedValue.value;
                decodedValue = baAsn1.decodeApplicationTime(buffer, offset + len);
                len += decodedValue.len;
                const time = decodedValue.value;
                value.eventTimeStamps[i] = new Date(date.getFullYear(), date.getMonth(), date.getDate(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds());
            }
            len++;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.notifyType = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeBitstring(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.eventEnable = decodedValue.value;
            len++;
            value.eventPriorities = [];
            for (let i = 0; i < 3; i++) {
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                value.eventPriorities[i] = decodedValue.value;
            }
            len++;
            alarms.push(value);
        }
        const moreEvents = buffer[apduLen - 1] === 1;
        return {
            len,
            alarms,
            moreEvents,
        };
    }
}
exports.default = EventInformation;
//# sourceMappingURL=EventInformation.js.map