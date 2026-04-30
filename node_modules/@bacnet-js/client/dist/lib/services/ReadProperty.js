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
class ReadProperty extends AbstractServices_1.BacnetService {
    static encode(buffer, objectType, objectInstance, propertyId, arrayIndex) {
        if (objectType <= enum_1.ASN1_MAX_OBJECT) {
            baAsn1.encodeContextObjectId(buffer, 0, objectType, objectInstance);
        }
        if (propertyId <= enum_1.ASN1_MAX_PROPERTY_ID) {
            baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        }
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex || arrayIndex === 0 ? arrayIndex : enum_1.ASN1_ARRAY_ALL);
        }
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        if (apduLen < 7)
            return undefined;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
            return undefined;
        len++;
        const objectIdResult = baAsn1.decodeObjectId(buffer, offset + len);
        len += objectIdResult.len;
        const objectId = {
            type: objectIdResult.objectType,
            instance: objectIdResult.instance,
        };
        const property = {
            id: 0,
            index: enum_1.ASN1_ARRAY_ALL,
        };
        const result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== 1)
            return undefined;
        const enumResult = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += enumResult.len;
        property.id = enumResult.value;
        if (len < apduLen) {
            const tagResult = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            len += tagResult.len;
            if (tagResult.tagNumber === 2 && len < apduLen) {
                const unsignedResult = baAsn1.decodeUnsigned(buffer, offset + len, tagResult.value);
                len += unsignedResult.len;
                property.index = unsignedResult.value;
            }
            else {
                return undefined;
            }
        }
        if (len < apduLen)
            return undefined;
        return {
            len,
            objectId,
            property,
        };
    }
    static encodeAcknowledge(buffer, objectId, propertyId, arrayIndex, values) {
        baAsn1.encodeContextObjectId(buffer, 0, objectId.type, objectId.instance);
        baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex);
        }
        baAsn1.encodeOpeningTag(buffer, 3);
        values.forEach((value) => baAsn1.bacappEncodeApplicationData(buffer, value));
        baAsn1.encodeClosingTag(buffer, 3);
    }
    static decodeAcknowledge(buffer, offset, apduLen) {
        const objectId = { type: 0, instance: 0 };
        const property = { id: 0, index: enum_1.ASN1_ARRAY_ALL };
        if (!baAsn1.decodeIsContextTag(buffer, offset, 0))
            return undefined;
        let len = 1;
        const objectIdResult = baAsn1.decodeObjectId(buffer, offset + len);
        len += objectIdResult.len;
        objectId.type = objectIdResult.objectType;
        objectId.instance = objectIdResult.instance;
        const tagResult = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += tagResult.len;
        if (tagResult.tagNumber !== 1)
            return undefined;
        const enumResult = baAsn1.decodeEnumerated(buffer, offset + len, tagResult.value);
        len += enumResult.len;
        property.id = enumResult.value;
        const indexTagResult = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        if (indexTagResult.tagNumber === 2) {
            len += indexTagResult.len;
            const unsignedResult = baAsn1.decodeUnsigned(buffer, offset + len, indexTagResult.value);
            len += unsignedResult.len;
            property.index = unsignedResult.value;
        }
        else {
            property.index = enum_1.ASN1_ARRAY_ALL;
        }
        const values = [];
        if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 3))
            return;
        len++;
        if (objectId.type === enum_1.ObjectType.SCHEDULE &&
            property.id === enum_1.PropertyIdentifier.WEEKLY_SCHEDULE) {
            const result = baAsn1.decodeWeeklySchedule(buffer, offset + len, apduLen - len);
            if (!result)
                return undefined;
            values.push({
                type: enum_1.ApplicationTag.WEEKLY_SCHEDULE,
                value: result.value,
            });
            len += result.len;
        }
        else if (objectId.type === enum_1.ObjectType.SCHEDULE &&
            property.id === enum_1.PropertyIdentifier.EXCEPTION_SCHEDULE) {
            const result = baAsn1.decodeExceptionSchedule(buffer, offset + len, apduLen - len);
            if (!result)
                return undefined;
            values.push({
                type: enum_1.ApplicationTag.SPECIAL_EVENT,
                value: result.value,
            });
            len += result.len;
        }
        else if (objectId.type === enum_1.ObjectType.SCHEDULE &&
            property.id === enum_1.PropertyIdentifier.EFFECTIVE_PERIOD) {
            const result = baAsn1.decodeScheduleEffectivePeriod(buffer, offset + len, apduLen - len);
            if (!result)
                return undefined;
            values.push({
                type: enum_1.ApplicationTag.DATERANGE,
                value: result.value,
            });
            len += result.len;
        }
        else if (objectId.type === enum_1.ObjectType.CALENDAR &&
            property.id === enum_1.PropertyIdentifier.DATE_LIST) {
            const result = baAsn1.decodeCalendarDatelist(buffer, offset + len, apduLen - len);
            if (!result)
                return undefined;
            values.push({
                type: enum_1.ApplicationTag.CALENDAR_ENTRY,
                value: result.value,
            });
            len += result.len;
        }
        else {
            while (apduLen - len > 1) {
                const result = baAsn1.bacappDecodeApplicationData(buffer, offset + len, apduLen + offset, objectId.type, property.id);
                if (!result)
                    return undefined;
                len += result.len;
                delete result.len;
                values.push(result);
            }
            if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 3))
                return;
            len++;
        }
        return {
            len,
            objectId,
            property,
            values,
        };
    }
}
exports.default = ReadProperty;
//# sourceMappingURL=ReadProperty.js.map