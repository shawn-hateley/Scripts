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
class AtomicWriteFile extends AbstractServices_1.BacnetAckService {
    static encode(buffer, isStream, objectId, position, blocks) {
        baAsn1.encodeApplicationObjectId(buffer, objectId.type, objectId.instance);
        if (isStream) {
            baAsn1.encodeOpeningTag(buffer, 0);
            baAsn1.encodeApplicationSigned(buffer, position);
            baAsn1.encodeApplicationOctetString(buffer, blocks[0], 0, blocks[0].length);
            baAsn1.encodeClosingTag(buffer, 0);
        }
        else {
            baAsn1.encodeOpeningTag(buffer, 1);
            baAsn1.encodeApplicationSigned(buffer, position);
            baAsn1.encodeApplicationUnsigned(buffer, blocks.length);
            for (let i = 0; i < blocks.length; i++) {
                baAsn1.encodeApplicationOctetString(buffer, blocks[i], 0, blocks[i].length);
            }
            baAsn1.encodeClosingTag(buffer, 1);
        }
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        let isStream = false;
        let position = 0;
        const blocks = [];
        let blockCount = 0;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== enum_1.ApplicationTag.OBJECTIDENTIFIER)
            return undefined;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        const objectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        if (baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 0)) {
            isStream = true;
            len++;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber !== enum_1.ApplicationTag.SIGNED_INTEGER)
                return undefined;
            decodedValue = baAsn1.decodeSigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            position = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber !== enum_1.ApplicationTag.OCTET_STRING)
                return undefined;
            decodedValue = baAsn1.decodeOctetString(buffer, offset + len, apduLen, 0, result.value);
            len += decodedValue.len;
            blocks.push(decodedValue.value);
            if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 0))
                return undefined;
            len++;
        }
        else if (baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 1)) {
            isStream = false;
            len++;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber !== enum_1.ApplicationTag.SIGNED_INTEGER)
                return undefined;
            decodedValue = baAsn1.decodeSigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            position = decodedValue.value;
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            if (result.tagNumber !== enum_1.ApplicationTag.UNSIGNED_INTEGER)
                return undefined;
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            blockCount = decodedValue.value;
            for (let i = 0; i < blockCount; i++) {
                result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                len += result.len;
                if (result.tagNumber !== enum_1.ApplicationTag.OCTET_STRING)
                    return undefined;
                decodedValue = baAsn1.decodeOctetString(buffer, offset + len, apduLen, 0, result.value);
                len += decodedValue.len;
                blocks.push(decodedValue.value);
            }
            if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 1))
                return undefined;
            len++;
        }
        else {
            return undefined;
        }
        return {
            len,
            isStream,
            objectId,
            position,
            blocks,
        };
    }
    static encodeAcknowledge(buffer, isStream, position) {
        if (isStream) {
            baAsn1.encodeContextSigned(buffer, 0, position);
        }
        else {
            baAsn1.encodeContextSigned(buffer, 1, position);
        }
    }
    static decodeAcknowledge(buffer, offset) {
        let len = 0;
        let isStream = false;
        let position = 0;
        const result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber === 0) {
            isStream = true;
        }
        else if (result.tagNumber === 1) {
            isStream = false;
        }
        else {
            return undefined;
        }
        const decodedValue = baAsn1.decodeSigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        position = decodedValue.value;
        return {
            len,
            isStream,
            position,
        };
    }
}
exports.default = AtomicWriteFile;
//# sourceMappingURL=AtomicWriteFile.js.map