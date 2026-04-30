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
class WhoHas extends AbstractServices_1.BacnetService {
    static encode(buffer, lowLimit, highLimit, objectId, objectName) {
        if (lowLimit >= 0 &&
            lowLimit <= enum_1.ASN1_MAX_INSTANCE &&
            highLimit >= 0 &&
            highLimit <= enum_1.ASN1_MAX_INSTANCE) {
            baAsn1.encodeContextUnsigned(buffer, 0, lowLimit);
            baAsn1.encodeContextUnsigned(buffer, 1, highLimit);
        }
        if (objectName && objectName !== '') {
            baAsn1.encodeContextCharacterString(buffer, 3, objectName);
        }
        else {
            baAsn1.encodeContextObjectId(buffer, 2, objectId.type, objectId.instance);
        }
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        const value = {};
        let decodedValue;
        let result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber === 0) {
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            if (decodedValue.value <= enum_1.ASN1_MAX_INSTANCE) {
                value.lowLimit = decodedValue.value;
            }
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
        }
        if (result.tagNumber === 1) {
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            if (decodedValue.value <= enum_1.ASN1_MAX_INSTANCE) {
                value.highLimit = decodedValue.value;
            }
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
        }
        if (result.tagNumber === 2) {
            decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
            len += decodedValue.len;
            value.objectId = {
                type: decodedValue.objectType,
                instance: decodedValue.instance,
            };
        }
        if (result.tagNumber === 3) {
            decodedValue = baAsn1.decodeCharacterString(buffer, offset + len, apduLen - (offset + len), result.value);
            len += decodedValue.len;
            value.objectName = decodedValue.value;
        }
        value.len = len;
        return value;
    }
}
exports.default = WhoHas;
//# sourceMappingURL=WhoHas.js.map