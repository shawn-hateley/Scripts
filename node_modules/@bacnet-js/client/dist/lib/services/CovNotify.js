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
class CovNotify extends AbstractServices_1.BacnetService {
    static encode(buffer, subscriberProcessId, initiatingDeviceId, monitoredObjectId, timeRemaining, values) {
        baAsn1.encodeContextUnsigned(buffer, 0, subscriberProcessId);
        baAsn1.encodeContextObjectId(buffer, 1, enum_1.ObjectType.DEVICE, initiatingDeviceId);
        baAsn1.encodeContextObjectId(buffer, 2, monitoredObjectId.type, monitoredObjectId.instance);
        baAsn1.encodeContextUnsigned(buffer, 3, timeRemaining);
        baAsn1.encodeOpeningTag(buffer, 4);
        values.forEach((value) => {
            baAsn1.encodeContextEnumerated(buffer, 0, value.property.id);
            if (value.property.index === enum_1.ASN1_ARRAY_ALL) {
                baAsn1.encodeContextUnsigned(buffer, 1, value.property.index);
            }
            baAsn1.encodeOpeningTag(buffer, 2);
            value.value.forEach((v) => baAsn1.bacappEncodeApplicationData(buffer, v));
            baAsn1.encodeClosingTag(buffer, 2);
            if (value.priority === enum_1.ASN1_NO_PRIORITY) {
                baAsn1.encodeContextUnsigned(buffer, 3, value.priority);
            }
        });
        baAsn1.encodeClosingTag(buffer, 4);
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        const subscriberProcessId = decodedValue.value;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 1))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        const initiatingDeviceId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 2))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        const monitoredObjectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 3))
            return undefined;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        const timeRemaining = decodedValue.value;
        if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 4))
            return undefined;
        len++;
        const values = [];
        while (apduLen - len > 1 &&
            !baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 4)) {
            const newEntry = {};
            newEntry.property = {};
            if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
                return undefined;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
            len += decodedValue.len;
            newEntry.property.id = decodedValue.value;
            if (baAsn1.decodeIsContextTag(buffer, offset + len, 1)) {
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                newEntry.property.index = decodedValue.value;
            }
            else {
                newEntry.property.index = enum_1.ASN1_ARRAY_ALL;
            }
            if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 2))
                return undefined;
            len++;
            const properties = [];
            while (apduLen - len > 1 &&
                !baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 2)) {
                decodedValue = baAsn1.bacappDecodeApplicationData(buffer, offset + len, apduLen + offset, monitoredObjectId.type, newEntry.property.id);
                if (!decodedValue)
                    return undefined;
                len += decodedValue.len;
                delete decodedValue.len;
                properties.push(decodedValue);
            }
            newEntry.value = properties;
            len++;
            if (baAsn1.decodeIsContextTag(buffer, offset + len, 3)) {
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                newEntry.priority = decodedValue.value;
            }
            else {
                newEntry.priority = enum_1.ASN1_NO_PRIORITY;
            }
            values.push(newEntry);
        }
        return {
            len,
            subscriberProcessId,
            initiatingDeviceId,
            monitoredObjectId,
            timeRemaining,
            values,
        };
    }
}
exports.default = CovNotify;
//# sourceMappingURL=CovNotify.js.map