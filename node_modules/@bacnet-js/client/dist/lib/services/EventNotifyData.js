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
class EventNotifyData extends AbstractServices_1.BacnetService {
    static encode(buffer, data) {
        baAsn1.encodeContextUnsigned(buffer, 0, data.processId);
        baAsn1.encodeContextObjectId(buffer, 1, data.initiatingObjectId.type, data.initiatingObjectId.instance);
        baAsn1.encodeContextObjectId(buffer, 2, data.eventObjectId.type, data.eventObjectId.instance);
        baAsn1.bacappEncodeContextTimestamp(buffer, 3, data.timeStamp);
        baAsn1.encodeContextUnsigned(buffer, 4, data.notificationClass);
        baAsn1.encodeContextUnsigned(buffer, 5, data.priority);
        baAsn1.encodeContextEnumerated(buffer, 6, data.eventType);
        if (data.messageText && data.messageText !== '') {
            baAsn1.encodeContextCharacterString(buffer, 7, data.messageText);
        }
        baAsn1.encodeContextEnumerated(buffer, 8, data.notifyType);
        switch (data.notifyType) {
            case enum_1.NotifyType.ALARM:
            case enum_1.NotifyType.EVENT:
                baAsn1.encodeContextBoolean(buffer, 9, data.ackRequired);
                baAsn1.encodeContextEnumerated(buffer, 10, data.fromState);
                break;
            default:
                break;
        }
        baAsn1.encodeContextEnumerated(buffer, 11, data.toState);
        switch (data.notifyType) {
            case enum_1.NotifyType.ALARM:
            case enum_1.NotifyType.EVENT:
                baAsn1.encodeOpeningTag(buffer, 12);
                switch (data.eventType) {
                    case enum_1.EventType.CHANGE_OF_BITSTRING:
                        baAsn1.encodeOpeningTag(buffer, 0);
                        baAsn1.encodeContextBitstring(buffer, 0, data.changeOfBitstringReferencedBitString);
                        baAsn1.encodeContextBitstring(buffer, 1, data.changeOfBitstringStatusFlags);
                        baAsn1.encodeClosingTag(buffer, 0);
                        break;
                    case enum_1.EventType.CHANGE_OF_STATE:
                        baAsn1.encodeOpeningTag(buffer, 1);
                        baAsn1.encodeOpeningTag(buffer, 0);
                        baAsn1.bacappEncodePropertyState(buffer, data.changeOfStateNewState);
                        baAsn1.encodeClosingTag(buffer, 0);
                        baAsn1.encodeContextBitstring(buffer, 1, data.changeOfStateStatusFlags);
                        baAsn1.encodeClosingTag(buffer, 1);
                        break;
                    case enum_1.EventType.CHANGE_OF_VALUE:
                        baAsn1.encodeOpeningTag(buffer, 2);
                        baAsn1.encodeOpeningTag(buffer, 0);
                        switch (data.changeOfValueTag) {
                            case enum_1.CovType.REAL:
                                baAsn1.encodeContextReal(buffer, 1, data.changeOfValueChangeValue);
                                break;
                            case enum_1.CovType.BIT_STRING:
                                baAsn1.encodeContextBitstring(buffer, 0, data.changeOfValueChangedBits);
                                break;
                            default:
                                throw new Error('NotImplemented');
                        }
                        baAsn1.encodeClosingTag(buffer, 0);
                        baAsn1.encodeContextBitstring(buffer, 1, data.changeOfValueStatusFlags);
                        baAsn1.encodeClosingTag(buffer, 2);
                        break;
                    case enum_1.EventType.FLOATING_LIMIT:
                        baAsn1.encodeOpeningTag(buffer, 4);
                        baAsn1.encodeContextReal(buffer, 0, data.floatingLimitReferenceValue);
                        baAsn1.encodeContextBitstring(buffer, 1, data.floatingLimitStatusFlags);
                        baAsn1.encodeContextReal(buffer, 2, data.floatingLimitSetPointValue);
                        baAsn1.encodeContextReal(buffer, 3, data.floatingLimitErrorLimit);
                        baAsn1.encodeClosingTag(buffer, 4);
                        break;
                    case enum_1.EventType.OUT_OF_RANGE:
                        baAsn1.encodeOpeningTag(buffer, 5);
                        baAsn1.encodeContextReal(buffer, 0, data.outOfRangeExceedingValue);
                        baAsn1.encodeContextBitstring(buffer, 1, data.outOfRangeStatusFlags);
                        baAsn1.encodeContextReal(buffer, 2, data.outOfRangeDeadband);
                        baAsn1.encodeContextReal(buffer, 3, data.outOfRangeExceededLimit);
                        baAsn1.encodeClosingTag(buffer, 5);
                        break;
                    case enum_1.EventType.CHANGE_OF_LIFE_SAFETY:
                        baAsn1.encodeOpeningTag(buffer, 8);
                        baAsn1.encodeContextEnumerated(buffer, 0, data.changeOfLifeSafetyNewState);
                        baAsn1.encodeContextEnumerated(buffer, 1, data.changeOfLifeSafetyNewMode);
                        baAsn1.encodeContextBitstring(buffer, 2, data.changeOfLifeSafetyStatusFlags);
                        baAsn1.encodeContextEnumerated(buffer, 3, data.changeOfLifeSafetyOperationExpected);
                        baAsn1.encodeClosingTag(buffer, 8);
                        break;
                    case enum_1.EventType.BUFFER_READY:
                        baAsn1.encodeOpeningTag(buffer, 10);
                        baAsn1.bacappEncodeContextDeviceObjPropertyRef(buffer, 0, data.bufferReadyBufferProperty);
                        baAsn1.encodeContextUnsigned(buffer, 1, data.bufferReadyPreviousNotification);
                        baAsn1.encodeContextUnsigned(buffer, 2, data.bufferReadyCurrentNotification);
                        baAsn1.encodeClosingTag(buffer, 10);
                        break;
                    case enum_1.EventType.UNSIGNED_RANGE:
                        baAsn1.encodeOpeningTag(buffer, 11);
                        baAsn1.encodeContextUnsigned(buffer, 0, data.unsignedRangeExceedingValue);
                        baAsn1.encodeContextBitstring(buffer, 1, data.unsignedRangeStatusFlags);
                        baAsn1.encodeContextUnsigned(buffer, 2, data.unsignedRangeExceededLimit);
                        baAsn1.encodeClosingTag(buffer, 11);
                        break;
                    case enum_1.EventType.EXTENDED:
                    case enum_1.EventType.COMMAND_FAILURE:
                        throw new Error('NotImplemented');
                    default:
                        throw new Error('NotImplemented');
                }
                baAsn1.encodeClosingTag(buffer, 12);
                break;
            case enum_1.NotifyType.ACK_NOTIFICATION:
                throw new Error('NotImplemented');
            default:
                break;
        }
    }
    static decode(buffer, offset) {
        let len = 0;
        let result;
        let decodedValue;
        const eventData = {};
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        eventData.processId = decodedValue.value;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 1))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        eventData.initiatingObjectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 2))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        eventData.eventObjectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 3))
            return undefined;
        len += 2;
        decodedValue = baAsn1.decodeApplicationDate(buffer, offset + len);
        len += decodedValue.len;
        const date = decodedValue.value;
        decodedValue = baAsn1.decodeApplicationTime(buffer, offset + len);
        len += decodedValue.len;
        const time = decodedValue.value;
        eventData.timeStamp = {
            type: enum_1.TimeStamp.DATETIME,
            value: new Date(date.getFullYear(), date.getMonth(), date.getDate(), time.getHours(), time.getMinutes(), time.getSeconds(), time.getMilliseconds()),
        };
        len += 2;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 4))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        eventData.notificationClass = decodedValue.value;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 5))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        eventData.priority = decodedValue.value;
        if (eventData.priority > 0xff)
            return undefined;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 6))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        eventData.eventType = decodedValue.value;
        if (baAsn1.decodeIsContextTag(buffer, offset + len, 7)) {
            decodedValue = baAsn1.decodeContextCharacterString(buffer, offset + len, 20000, 7);
            len += decodedValue.len;
            eventData.messageText = decodedValue.value;
        }
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 8))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        eventData.notifyType = decodedValue.value;
        switch (eventData.notifyType) {
            case enum_1.NotifyType.ALARM:
            case enum_1.NotifyType.EVENT:
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, 1);
                len += decodedValue.len;
                eventData.ackRequired = decodedValue.value > 0;
                if (!baAsn1.decodeIsContextTag(buffer, offset + len, 10))
                    return undefined;
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
                len += decodedValue.len;
                eventData.fromState = decodedValue.value;
                break;
            default:
                break;
        }
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 11))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        eventData.toState = decodedValue.value;
        eventData.len = len;
        return eventData;
    }
}
exports.default = EventNotifyData;
//# sourceMappingURL=EventNotifyData.js.map