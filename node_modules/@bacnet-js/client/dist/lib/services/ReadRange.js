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
class ReadRange extends AbstractServices_1.BacnetAckService {
    static encode(buffer, objectId, propertyId, arrayIndex, requestType, position, time, count) {
        baAsn1.encodeContextObjectId(buffer, 0, objectId.type, objectId.instance);
        baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex);
        }
        switch (requestType) {
            case enum_1.ReadRangeType.BY_POSITION:
                baAsn1.encodeOpeningTag(buffer, 3);
                baAsn1.encodeApplicationUnsigned(buffer, position);
                baAsn1.encodeApplicationSigned(buffer, count);
                baAsn1.encodeClosingTag(buffer, 3);
                break;
            case enum_1.ReadRangeType.BY_SEQUENCE_NUMBER:
                baAsn1.encodeOpeningTag(buffer, 6);
                baAsn1.encodeApplicationUnsigned(buffer, position);
                baAsn1.encodeApplicationSigned(buffer, count);
                baAsn1.encodeClosingTag(buffer, 6);
                break;
            case enum_1.ReadRangeType.BY_TIME_REFERENCE_TIME_COUNT:
                baAsn1.encodeOpeningTag(buffer, 7);
                baAsn1.encodeApplicationDate(buffer, time);
                baAsn1.encodeApplicationTime(buffer, time);
                baAsn1.encodeApplicationSigned(buffer, count);
                baAsn1.encodeClosingTag(buffer, 7);
                break;
            default:
                break;
        }
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        let requestType;
        let position;
        let time;
        let count;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
            return undefined;
        len++;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        const objectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        const property = {};
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== 1)
            return undefined;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        property.id = decodedValue.value;
        if (len < apduLen &&
            baAsn1.decodeIsContextTag(buffer, offset + len, 2)) {
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += result.len;
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            property.index = decodedValue.value;
        }
        else {
            property.index = enum_1.ASN1_ARRAY_ALL;
        }
        if (len < apduLen) {
            result = baAsn1.decodeTagNumber(buffer, offset + len);
            len += result.len;
            switch (result.tagNumber) {
                case 3: {
                    requestType = enum_1.ReadRangeType.BY_POSITION;
                    result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                    len += result.len;
                    decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                    len += decodedValue.len;
                    position = decodedValue.value;
                    result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                    len += result.len;
                    decodedValue = baAsn1.decodeSigned(buffer, offset + len, result.value);
                    len += decodedValue.len;
                    count = decodedValue.value;
                    break;
                }
                case 6: {
                    requestType = enum_1.ReadRangeType.BY_SEQUENCE_NUMBER;
                    result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                    len += result.len;
                    decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                    len += decodedValue.len;
                    position = decodedValue.value;
                    result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                    len += result.len;
                    decodedValue = baAsn1.decodeSigned(buffer, offset + len, result.value);
                    len += decodedValue.len;
                    count = decodedValue.value;
                    break;
                }
                case 7: {
                    requestType = enum_1.ReadRangeType.BY_TIME_REFERENCE_TIME_COUNT;
                    decodedValue = baAsn1.decodeApplicationDate(buffer, offset + len);
                    len += decodedValue.len;
                    const tmpDate = decodedValue.value;
                    decodedValue = baAsn1.decodeApplicationTime(buffer, offset + len);
                    len += decodedValue.len;
                    const tmpTime = decodedValue.value;
                    time = new Date(tmpDate.getFullYear(), tmpDate.getMonth(), tmpDate.getDate(), tmpTime.getHours(), tmpTime.getMinutes(), tmpTime.getSeconds(), tmpTime.getMilliseconds());
                    result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
                    len += result.len;
                    decodedValue = baAsn1.decodeSigned(buffer, offset + len, result.value);
                    len += decodedValue.len;
                    count = decodedValue.value;
                    break;
                }
                default:
                    return undefined;
            }
            result = baAsn1.decodeTagNumber(buffer, offset + len);
            len += result.len;
        }
        return {
            len,
            objectId,
            property,
            requestType,
            position,
            time,
            count,
        };
    }
    static encodeAcknowledge(buffer, objectId, propertyId, arrayIndex, resultFlags, itemCount, applicationData, requestType, firstSequence) {
        baAsn1.encodeContextObjectId(buffer, 0, objectId.type, objectId.instance);
        baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex);
        }
        baAsn1.encodeContextBitstring(buffer, 3, resultFlags);
        baAsn1.encodeContextUnsigned(buffer, 4, itemCount);
        baAsn1.encodeOpeningTag(buffer, 5);
        if (itemCount !== 0) {
            applicationData.copy(buffer.buffer, buffer.offset, 0, applicationData.length);
            buffer.offset += applicationData.length;
        }
        baAsn1.encodeClosingTag(buffer, 5);
        if (itemCount !== 0 &&
            requestType &&
            requestType !== enum_1.ReadRangeType.BY_POSITION) {
            baAsn1.encodeContextUnsigned(buffer, 6, firstSequence);
        }
    }
    static decodeAcknowledge(buffer, offset, apduLen) {
        let len = 0;
        let result;
        let decodedValue;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
            return undefined;
        len++;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        len += decodedValue.len;
        const objectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        const property = { index: enum_1.ASN1_ARRAY_ALL };
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== 1)
            return undefined;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        property.id = decodedValue.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        if (result.tagNumber === 2 && len < apduLen) {
            len += result.len;
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            property.index = decodedValue.value;
        }
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeBitstring(buffer, offset + len, result.value);
        len += decodedValue.len;
        const resultFlag = decodedValue.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
        len += decodedValue.len;
        const itemCount = decodedValue.value;
        if (!baAsn1.decodeIsOpeningTag(buffer, offset + len))
            return undefined;
        len++;
        const decodedRange = baAsn1.decodeRange(buffer, offset + len, offset + apduLen);
        const rangeStart = offset + len;
        let rangeEnd;
        if (decodedRange) {
            rangeEnd = rangeStart + decodedRange.len;
        }
        else {
            const searchEnd = offset + apduLen;
            let closingTagOffset = -1;
            for (let i = rangeStart; i < searchEnd; i++) {
                if (baAsn1.decodeIsClosingTagNumber(buffer, i, 5)) {
                    closingTagOffset = i;
                    break;
                }
            }
            if (closingTagOffset === -1)
                return undefined;
            rangeEnd = closingTagOffset;
        }
        const rangeBuffer = buffer.slice(rangeStart, rangeEnd);
        return {
            objectId,
            property,
            resultFlag,
            itemCount,
            rangeBuffer,
            ...(decodedRange ? { values: decodedRange.value } : {}),
            len,
        };
    }
}
exports.default = ReadRange;
//# sourceMappingURL=ReadRange.js.map