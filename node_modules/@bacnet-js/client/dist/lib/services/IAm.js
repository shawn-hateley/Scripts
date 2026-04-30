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
class IAm extends AbstractServices_1.BacnetService {
    static encode(buffer, deviceId, maxApdu, segmentation, vendorId) {
        baAsn1.encodeApplicationObjectId(buffer, enum_1.ObjectType.DEVICE, deviceId);
        baAsn1.encodeApplicationUnsigned(buffer, maxApdu);
        baAsn1.encodeApplicationEnumerated(buffer, segmentation);
        baAsn1.encodeApplicationUnsigned(buffer, vendorId);
    }
    static decode(buffer, offset) {
        let result;
        let apduLen = 0;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + apduLen);
        apduLen += result.len;
        if (result.tagNumber !== enum_1.ApplicationTag.OBJECTIDENTIFIER)
            return undefined;
        result = baAsn1.decodeObjectId(buffer, offset + apduLen);
        apduLen += result.len;
        if (result.objectType !== enum_1.ObjectType.DEVICE)
            return undefined;
        const deviceId = result.instance;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + apduLen);
        apduLen += result.len;
        if (result.tagNumber !== enum_1.ApplicationTag.UNSIGNED_INTEGER)
            return undefined;
        result = baAsn1.decodeUnsigned(buffer, offset + apduLen, result.value);
        apduLen += result.len;
        const maxApdu = result.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + apduLen);
        apduLen += result.len;
        if (result.tagNumber !== enum_1.ApplicationTag.ENUMERATED)
            return undefined;
        result = baAsn1.decodeEnumerated(buffer, offset + apduLen, result.value);
        apduLen += result.len;
        if (result.value > enum_1.Segmentation.NO_SEGMENTATION)
            return undefined;
        const segmentation = result.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + apduLen);
        apduLen += result.len;
        if (result.tagNumber !== enum_1.ApplicationTag.UNSIGNED_INTEGER)
            return undefined;
        result = baAsn1.decodeUnsigned(buffer, offset + apduLen, result.value);
        apduLen += result.len;
        if (result.value > 0xffff)
            return undefined;
        const vendorId = result.value;
        return {
            len: apduLen,
            deviceId,
            maxApdu,
            segmentation,
            vendorId,
        };
    }
}
exports.default = IAm;
//# sourceMappingURL=IAm.js.map