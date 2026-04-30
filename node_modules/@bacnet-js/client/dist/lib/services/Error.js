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
class ErrorService extends AbstractServices_1.BacnetService {
    static encode(buffer, errorClass, errorCode) {
        baAsn1.encodeApplicationEnumerated(buffer, errorClass);
        baAsn1.encodeApplicationEnumerated(buffer, errorCode);
    }
    static decode(buffer, offset) {
        const orgOffset = offset;
        let result;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset);
        offset += result.len;
        const errorClass = baAsn1.decodeEnumerated(buffer, offset, result.value);
        offset += errorClass.len;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset);
        offset += result.len;
        const errorCode = baAsn1.decodeEnumerated(buffer, offset, result.value);
        offset += errorClass.len;
        return {
            len: offset - orgOffset,
            class: errorClass.value,
            code: errorCode.value,
        };
    }
    static buildMessage(result) {
        return (`BacnetError Class: ${enum_1.ErrorClass[result.class]} ` +
            `(${result.class}) ` +
            `Code: ${enum_1.ErrorCode[result.code]} (${result.code})`);
    }
}
exports.default = ErrorService;
//# sourceMappingURL=Error.js.map