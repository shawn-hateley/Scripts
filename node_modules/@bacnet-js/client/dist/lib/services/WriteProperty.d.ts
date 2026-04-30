import { EncodeBuffer, WritePropertyRequest, BACNetWritePropertyValues } from '../types';
import { BacnetService } from './AbstractServices';
export default class WriteProperty extends BacnetService {
    private static validateRawDateByte;
    private static validateWeekNDayByte;
    private static isObjectRecord;
    private static hasTypeAndValue;
    private static isRawDate;
    private static hasRawDate;
    private static isDateAppData;
    private static isTimeAppData;
    private static isWeekNDayAppData;
    private static writeDateBytes;
    private static extractDateInput;
    private static normalizeTimeInput;
    private static encodeDate;
    private static encodeWeekNDayContext;
    private static encodeDateRangeContext;
    private static encodeWriteHeader;
    private static encodeWritePriority;
    private static encodeWeeklySchedulePayload;
    private static encodeExceptionDate;
    private static encodeExceptionSchedulePayload;
    private static encodeEffectivePeriodPayload;
    private static encodeCalendarDateListPayload;
    static encode(buffer: EncodeBuffer, objectType: number, objectInstance: number, propertyId: number, arrayIndex: number, priority: number, values: BACNetWritePropertyValues): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): WritePropertyRequest | undefined;
}
