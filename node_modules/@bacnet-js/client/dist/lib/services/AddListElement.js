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
class AddListElement extends AbstractServices_1.BacnetService {
    static encode(buffer, objectId, propertyId, arrayIndex, values) {
        baAsn1.encodeContextObjectId(buffer, 0, objectId.type, objectId.instance);
        baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex);
        }
        baAsn1.encodeOpeningTag(buffer, 3);
        values.forEach((value) => baAsn1.bacappEncodeApplicationData(buffer, value));
        baAsn1.encodeClosingTag(buffer, 3);
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        const value = {
            len: 0,
            objectId: { type: 0, instance: 0 },
            property: {
                id: 0,
                index: enum_1.ASN1_ARRAY_ALL,
            },
            values: [],
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
        value.property.id = decodedValue.value;
        if (len < apduLen &&
            baAsn1.decodeIsContextTag(buffer, offset + len, 2)) {
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.property.index = decodedValue.value;
        }
        if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 3))
            return undefined;
        len++;
        const values = [];
        while (apduLen - len > 1) {
            result = baAsn1.bacappDecodeApplicationData(buffer, offset + len, apduLen + offset, value.objectId.type, value.property.id);
            if (!result)
                return undefined;
            len += result.len;
            delete result.len;
            values.push(result);
        }
        value.values = values;
        if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 3))
            return undefined;
        len++;
        value.len = len;
        return value;
    }
}
exports.default = AddListElement;
//# sourceMappingURL=AddListElement.js.map