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
class AlarmSummary extends AbstractServices_1.BacnetService {
    static encode(buffer, alarms) {
        alarms.forEach((alarm) => {
            baAsn1.encodeContextObjectId(buffer, 12, alarm.objectId.type, alarm.objectId.instance);
            baAsn1.encodeContextEnumerated(buffer, 9, alarm.alarmState);
            baAsn1.encodeContextBitstring(buffer, 8, alarm.acknowledgedTransitions);
        });
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        const alarms = [];
        while (apduLen - 3 - len > 0) {
            const value = {
                objectId: { type: 0, instance: 0 },
                alarmState: 0,
                acknowledgedTransitions: { bitsUsed: 0, value: [] },
            };
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
            value.alarmState = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeBitstring(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.acknowledgedTransitions = decodedValue.value;
            alarms.push(value);
        }
        return {
            len,
            alarms,
        };
    }
}
exports.default = AlarmSummary;
//# sourceMappingURL=AlarmSummary.js.map