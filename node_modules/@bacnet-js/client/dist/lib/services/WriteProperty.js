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
class WriteProperty extends AbstractServices_1.BacnetService {
    static validateRawDateByte(name, value, min, max) {
        if (!Number.isInteger(value) || value < min || value > max) {
            throw new Error(`invalid raw date ${name}: ${value}`);
        }
    }
    static validateWeekNDayByte(name, value, min, max) {
        if (value === 0xff)
            return;
        WriteProperty.validateRawDateByte(name, value, min, max);
    }
    static isObjectRecord(value) {
        return value != null && typeof value === 'object';
    }
    static hasTypeAndValue(value) {
        return (WriteProperty.isObjectRecord(value) &&
            'type' in value &&
            'value' in value);
    }
    static isRawDate(value) {
        return (WriteProperty.isObjectRecord(value) &&
            'year' in value &&
            'month' in value &&
            'day' in value &&
            'wday' in value);
    }
    static hasRawDate(value) {
        return WriteProperty.isObjectRecord(value) && 'raw' in value;
    }
    static isDateAppData(value) {
        return (WriteProperty.hasTypeAndValue(value) &&
            value.type === enum_1.ApplicationTag.DATE);
    }
    static isTimeAppData(value) {
        return (WriteProperty.hasTypeAndValue(value) &&
            value.type === enum_1.ApplicationTag.TIME);
    }
    static isWeekNDayAppData(value) {
        return (WriteProperty.hasTypeAndValue(value) &&
            value.type === enum_1.ApplicationTag.WEEKNDAY);
    }
    static writeDateBytes(buffer, value) {
        if (WriteProperty.isRawDate(value)) {
            WriteProperty.validateRawDateByte('year', value.year, 0, 255);
            if (value.month !== 0xff) {
                WriteProperty.validateRawDateByte('month', value.month, 1, 14);
            }
            if (value.day !== 0xff) {
                WriteProperty.validateRawDateByte('day', value.day, 1, 34);
            }
            if (value.wday !== 0xff) {
                WriteProperty.validateRawDateByte('wday', value.wday, 1, 7);
            }
            buffer.buffer[buffer.offset++] = value.year;
            buffer.buffer[buffer.offset++] = value.month;
            buffer.buffer[buffer.offset++] = value.day;
            buffer.buffer[buffer.offset++] = value.wday;
            return;
        }
        const isWildcardDate = value === baAsn1.ZERO_DATE;
        const date = value instanceof Date ? value : new Date(value);
        if (isWildcardDate) {
            buffer.buffer[buffer.offset++] = 0xff;
            buffer.buffer[buffer.offset++] = 0xff;
            buffer.buffer[buffer.offset++] = 0xff;
            buffer.buffer[buffer.offset++] = 0xff;
            return;
        }
        if (date.getFullYear() >= baAsn1.START_YEAR) {
            buffer.buffer[buffer.offset++] =
                date.getFullYear() - baAsn1.START_YEAR;
        }
        else if (date.getFullYear() < baAsn1.MAX_YEARS) {
            buffer.buffer[buffer.offset++] = date.getFullYear();
        }
        else {
            throw new Error(`invalid year: ${date.getFullYear()}`);
        }
        buffer.buffer[buffer.offset++] = date.getMonth() + 1;
        buffer.buffer[buffer.offset++] = date.getDate();
        buffer.buffer[buffer.offset++] = date.getDay() === 0 ? 7 : date.getDay();
    }
    static extractDateInput(entry) {
        if (WriteProperty.hasRawDate(entry)) {
            return entry.raw;
        }
        if (WriteProperty.isDateAppData(entry)) {
            return entry.value;
        }
        return entry;
    }
    static normalizeTimeInput(time, errorPrefix) {
        const timeValue = WriteProperty.isTimeAppData(time) ? time.value : time;
        if (timeValue == null) {
            throw new Error(`${errorPrefix} time is required`);
        }
        const normalized = timeValue instanceof Date ? timeValue : new Date(timeValue);
        if (Number.isNaN(normalized.getTime())) {
            throw new Error(`${errorPrefix} time is invalid`);
        }
        return normalized;
    }
    static encodeDate(buffer, value, contextTag) {
        if (contextTag !== undefined) {
            baAsn1.encodeTag(buffer, contextTag, true, 4);
        }
        else {
            baAsn1.encodeTag(buffer, enum_1.ApplicationTag.DATE, false, 4);
        }
        WriteProperty.writeDateBytes(buffer, value);
    }
    static encodeWeekNDayContext(buffer, value) {
        const weekNDay = WriteProperty.isWeekNDayAppData(value)
            ? value.value
            : value;
        if (!WriteProperty.isObjectRecord(weekNDay)) {
            throw new Error('Could not encode: invalid WEEKNDAY value');
        }
        WriteProperty.validateWeekNDayByte('month', weekNDay.month, 1, 14);
        WriteProperty.validateWeekNDayByte('week', weekNDay.week, 1, 6);
        WriteProperty.validateWeekNDayByte('wday', weekNDay.wday, 1, 7);
        baAsn1.encodeTag(buffer, 2, true, 3);
        buffer.buffer[buffer.offset++] = weekNDay.month;
        buffer.buffer[buffer.offset++] = weekNDay.week;
        buffer.buffer[buffer.offset++] = weekNDay.wday;
    }
    static encodeDateRangeContext(buffer, value, invalidMessage) {
        if (!Array.isArray(value) || value.length !== 2) {
            throw new Error(invalidMessage);
        }
        baAsn1.encodeOpeningTag(buffer, 1);
        for (const row of value) {
            WriteProperty.encodeDate(buffer, WriteProperty.extractDateInput(row));
        }
        baAsn1.encodeClosingTag(buffer, 1);
    }
    static encodeWriteHeader(buffer, objectType, objectInstance, propertyId, arrayIndex) {
        baAsn1.encodeContextObjectId(buffer, 0, objectType, objectInstance);
        baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex);
        }
        baAsn1.encodeOpeningTag(buffer, 3);
    }
    static encodeWritePriority(buffer, priority) {
        if (priority !== enum_1.ASN1_NO_PRIORITY) {
            baAsn1.encodeContextUnsigned(buffer, 4, priority);
        }
    }
    static encodeWeeklySchedulePayload(buffer, values) {
        if (!Array.isArray(values)) {
            throw new Error('Could not encode: weekly schedule should be an array');
        }
        if (values.length !== 7) {
            throw new Error('Could not encode: weekly schedule should have exactly 7 days');
        }
        for (const [index, day] of values.entries()) {
            if (!Array.isArray(day)) {
                throw new Error(`Could not encode: weekly schedule day ${index} should be an array`);
            }
            baAsn1.encodeOpeningTag(buffer, 0);
            for (const [slotIndex, slot] of day.entries()) {
                const timeValue = WriteProperty.normalizeTimeInput(slot?.time, `Could not encode: weekly schedule day ${index} slot ${slotIndex}`);
                baAsn1.bacappEncodeApplicationData(buffer, {
                    type: enum_1.ApplicationTag.TIME,
                    value: timeValue,
                });
                baAsn1.bacappEncodeApplicationData(buffer, slot.value);
            }
            baAsn1.encodeClosingTag(buffer, 0);
        }
    }
    static encodeExceptionDate(buffer, date) {
        if (date.type === enum_1.ApplicationTag.DATE) {
            WriteProperty.encodeDate(buffer, WriteProperty.extractDateInput(date), 0);
            return;
        }
        if (date.type === enum_1.ApplicationTag.DATERANGE) {
            WriteProperty.encodeDateRangeContext(buffer, date.value, 'Could not encode: exception schedule date range must have exactly 2 dates');
            return;
        }
        if (date.type === enum_1.ApplicationTag.WEEKNDAY) {
            WriteProperty.encodeWeekNDayContext(buffer, date);
            return;
        }
        throw new Error('Could not encode: unsupported exception schedule date format');
    }
    static encodeExceptionSchedulePayload(buffer, values) {
        if (!Array.isArray(values)) {
            throw new Error('Could not encode: exception schedule values must be an array');
        }
        for (const [index, entry] of values.entries()) {
            baAsn1.encodeOpeningTag(buffer, 0);
            WriteProperty.encodeExceptionDate(buffer, entry.date);
            baAsn1.encodeClosingTag(buffer, 0);
            const events = entry?.events;
            if (events != null && !Array.isArray(events)) {
                throw new Error(`Could not encode: exception schedule entry ${index} events must be an array`);
            }
            baAsn1.encodeOpeningTag(buffer, 2);
            for (const [eventIndex, event] of (events || []).entries()) {
                const timeValue = WriteProperty.normalizeTimeInput(event?.time, `Could not encode: exception schedule entry ${index} event ${eventIndex}`);
                baAsn1.bacappEncodeApplicationData(buffer, {
                    type: enum_1.ApplicationTag.TIME,
                    value: timeValue,
                });
                baAsn1.bacappEncodeApplicationData(buffer, event.value);
            }
            baAsn1.encodeClosingTag(buffer, 2);
            const priority = entry?.priority;
            const priorityValue = typeof priority === 'number' ? priority : priority?.value;
            if (!Number.isInteger(priorityValue) ||
                priorityValue < enum_1.ASN1_MIN_PRIORITY ||
                priorityValue > enum_1.ASN1_MAX_PRIORITY) {
                throw new Error(`Could not encode: exception schedule priority must be between ${enum_1.ASN1_MIN_PRIORITY} and ${enum_1.ASN1_MAX_PRIORITY}`);
            }
            baAsn1.encodeContextUnsigned(buffer, 3, priorityValue);
        }
    }
    static encodeEffectivePeriodPayload(buffer, values) {
        if (!Array.isArray(values)) {
            throw new Error('Could not encode: effective period should be an array');
        }
        if (values.length !== 2) {
            throw new Error('Could not encode: effective period should have a length of 2');
        }
        for (const entry of values) {
            WriteProperty.encodeDate(buffer, WriteProperty.extractDateInput(entry));
        }
    }
    static encodeCalendarDateListPayload(buffer, values) {
        if (!Array.isArray(values)) {
            throw new Error('Could not encode: calendar date list should be an array');
        }
        for (const entry of values) {
            if (entry?.type === enum_1.ApplicationTag.DATE) {
                WriteProperty.encodeDate(buffer, WriteProperty.extractDateInput(entry), 0);
            }
            else if (entry?.type === enum_1.ApplicationTag.DATERANGE) {
                WriteProperty.encodeDateRangeContext(buffer, entry.value, 'Could not encode: calendar date list date range must have exactly 2 dates');
            }
            else if (entry?.type === enum_1.ApplicationTag.WEEKNDAY) {
                WriteProperty.encodeWeekNDayContext(buffer, entry);
            }
            else {
                throw new Error('Could not encode: unsupported calendar date list entry format');
            }
        }
    }
    static encode(buffer, objectType, objectInstance, propertyId, arrayIndex, priority, values) {
        if (objectType === enum_1.ObjectType.SCHEDULE &&
            propertyId === enum_1.PropertyIdentifier.WEEKLY_SCHEDULE) {
            WriteProperty.encodeWriteHeader(buffer, objectType, objectInstance, propertyId, arrayIndex);
            WriteProperty.encodeWeeklySchedulePayload(buffer, values);
            baAsn1.encodeClosingTag(buffer, 3);
            WriteProperty.encodeWritePriority(buffer, priority);
            return;
        }
        if (objectType === enum_1.ObjectType.SCHEDULE &&
            propertyId === enum_1.PropertyIdentifier.EXCEPTION_SCHEDULE) {
            WriteProperty.encodeWriteHeader(buffer, objectType, objectInstance, propertyId, arrayIndex);
            WriteProperty.encodeExceptionSchedulePayload(buffer, values);
            baAsn1.encodeClosingTag(buffer, 3);
            WriteProperty.encodeWritePriority(buffer, priority);
            return;
        }
        if (objectType === enum_1.ObjectType.SCHEDULE &&
            propertyId === enum_1.PropertyIdentifier.EFFECTIVE_PERIOD) {
            WriteProperty.encodeWriteHeader(buffer, objectType, objectInstance, propertyId, arrayIndex);
            WriteProperty.encodeEffectivePeriodPayload(buffer, values);
            baAsn1.encodeClosingTag(buffer, 3);
            WriteProperty.encodeWritePriority(buffer, priority);
            return;
        }
        if (objectType === enum_1.ObjectType.CALENDAR &&
            propertyId === enum_1.PropertyIdentifier.DATE_LIST) {
            WriteProperty.encodeWriteHeader(buffer, objectType, objectInstance, propertyId, arrayIndex);
            WriteProperty.encodeCalendarDateListPayload(buffer, values);
            baAsn1.encodeClosingTag(buffer, 3);
            WriteProperty.encodeWritePriority(buffer, priority);
            return;
        }
        baAsn1.encodeContextObjectId(buffer, 0, objectType, objectInstance);
        baAsn1.encodeContextEnumerated(buffer, 1, propertyId);
        if (arrayIndex !== enum_1.ASN1_ARRAY_ALL) {
            baAsn1.encodeContextUnsigned(buffer, 2, arrayIndex);
        }
        baAsn1.encodeOpeningTag(buffer, 3);
        values.forEach((value) => baAsn1.bacappEncodeApplicationData(buffer, value));
        baAsn1.encodeClosingTag(buffer, 3);
        if (priority !== enum_1.ASN1_NO_PRIORITY) {
            baAsn1.encodeContextUnsigned(buffer, 4, priority);
        }
    }
    static decode(buffer, offset, apduLen) {
        let len = 0;
        const value = {
            property: { id: 0, index: enum_1.ASN1_ARRAY_ALL },
        };
        let decodedValue;
        let result;
        if (!baAsn1.decodeIsContextTag(buffer, offset + len, 0))
            return undefined;
        len++;
        decodedValue = baAsn1.decodeObjectId(buffer, offset + len);
        const objectId = {
            type: decodedValue.objectType,
            instance: decodedValue.instance,
        };
        len += decodedValue.len;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        len += result.len;
        if (result.tagNumber !== 1)
            return undefined;
        decodedValue = baAsn1.decodeEnumerated(buffer, offset + len, result.value);
        len += decodedValue.len;
        value.property.id = decodedValue.value;
        result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
        if (result.tagNumber === 2) {
            len += result.len;
            decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
            len += decodedValue.len;
            value.property.index = decodedValue.value;
        }
        else {
            value.property.index = enum_1.ASN1_ARRAY_ALL;
        }
        if (!baAsn1.decodeIsOpeningTagNumber(buffer, offset + len, 3))
            return undefined;
        len++;
        const values = [];
        while (apduLen - len > 1 &&
            !baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 3)) {
            decodedValue = baAsn1.bacappDecodeApplicationData(buffer, offset + len, apduLen + offset, objectId.type, value.property.id);
            if (!decodedValue)
                return undefined;
            len += decodedValue.len;
            delete decodedValue.len;
            values.push(decodedValue);
        }
        value.value = values;
        if (!baAsn1.decodeIsClosingTagNumber(buffer, offset + len, 3))
            return undefined;
        len++;
        value.priority = enum_1.ASN1_MAX_PRIORITY;
        if (len < apduLen) {
            result = baAsn1.decodeTagNumberAndValue(buffer, offset + len);
            if (result.tagNumber === 4) {
                len += result.len;
                decodedValue = baAsn1.decodeUnsigned(buffer, offset + len, result.value);
                len += decodedValue.len;
                if (decodedValue.value >= enum_1.ASN1_MIN_PRIORITY &&
                    decodedValue.value <= enum_1.ASN1_MAX_PRIORITY) {
                    value.priority = decodedValue.value;
                }
                else {
                    return undefined;
                }
            }
        }
        return {
            len,
            objectId,
            value: value,
        };
    }
}
exports.default = WriteProperty;
//# sourceMappingURL=WriteProperty.js.map