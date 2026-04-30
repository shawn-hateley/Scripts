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
class WritePropertyMultiple extends AbstractServices_1.BacnetService {
    static encode(buffer, objectId, values) {
        baAsn1.encodeContextObjectId(buffer, 0, objectId.type, objectId.instance);
        baAsn1.encodeOpeningTag(buffer, 1);
        values.forEach((pValue) => {
            baAsn1.encodeContextEnumerated(buffer, 0, pValue.property.id);
            if (pValue.property.index !== enum_1.ASN1_ARRAY_ALL) {
                baAsn1.encodeContextUnsigned(buffer, 1, pValue.property.index);
            }
            baAsn1.encodeOpeningTag(buffer, 2);
            pValue.value.forEach((value) => baAsn1.bacappEncodeApplicationData(buffer, value));
            baAsn1.encodeClosingTag(buffer, 2);
            if (pValue.priority !== enum_1.ASN1_NO_PRIORITY) {
                baAsn1.encodeContextUnsigned(buffer, 3, pValue.priority);
            }
        });
        baAsn1.encodeClosingTag(buffer, 1);
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== 0 || apduLen <= len)
            return undefined;
        apduLen -= len;
        if (apduLen < 4)
            return undefined;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        const objectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 1))
            return undefined;
        len++;
        const _values = [];
        while (apduLen - len > 1) {
            const newEntry = {};
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber !== 0)
                return undefined;
            decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
            len += decodedValue.len;
            const propertyId = decodedValue.value;
            let arrayIndex = enum_1.ASN1_ARRAY_ALL;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber === 1) {
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                arrayIndex = decodedValue.value;
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
            }
            newEntry.property = { id: propertyId, index: arrayIndex };
            if (result.tagNumber !== 2 ||
                !baAsn1.decodeIsOpeningTag(buffer, offset + len - 1))
                return undefined;
            const values = [];
            while (len + offset <= buffer.length &&
                !baAsn1.decodeIsClosingTag(buffer, offset + len)) {
                const value = baAsn1.bacappDecodeApplicationData(buffer, offset + len, apduLen + offset, objectId.type, propertyId);
                if (!value)
                    return undefined;
                len += value.len;
                delete value.len;
                values.push(value);
            }
            len++;
            newEntry.value = values;
            let priority = enum_1.ASN1_NO_PRIORITY;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber === 3) {
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                priority = decodedValue.value;
            }
            else {
                len--;
            }
            newEntry.priority = priority;
            _values.push(newEntry);
        }
        if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 1))
            return undefined;
        len++;
        return {
            len,
            objectId,
            values: _values,
        };
    }
    static encodeObject(buffer, values) {
        values.forEach((object) => WritePropertyMultiple.encode(buffer, object.objectId, object.values));
    }
}
exports.default = WritePropertyMultiple;
//# sourceMappingURL=WritePropertyMultiple.js.map