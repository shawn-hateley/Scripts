import { EncodeBuffer, EnrollmentFilter, EnrollmentSummary, EnrollmentSummaryAcknowledge, PriorityFilter } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class GetEnrollmentSummary extends BacnetAckService {
    static encode(buffer: EncodeBuffer, acknowledgmentFilter: number, enrollmentFilter?: EnrollmentFilter, eventStateFilter?: number, eventTypeFilter?: number, priorityFilter?: PriorityFilter, notificationClassFilter?: number): void;
    static decode(buffer: Buffer, offset: number): any;
    static encodeAcknowledge(buffer: EncodeBuffer, enrollmentSummaries: EnrollmentSummary[]): void;
    static decodeAcknowledge(buffer: Buffer, offset: number, apduLen: number): EnrollmentSummaryAcknowledge | undefined;
}
