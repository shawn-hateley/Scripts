import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetService } from './AbstractServices';
export default class AlarmAcknowledge extends BacnetService {
    static encode(buffer: EncodeBuffer, ackProcessId: number, eventObjectId: BACNetObjectID, eventStateAcknowledged: number, ackSource: string, eventTimeStamp: any, ackTimeStamp: any): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        acknowledgedProcessId: number;
        eventObjectId: BACNetObjectID;
        eventStateAcknowledged: number;
        eventTimeStamp: Date | number;
        acknowledgeSource: string;
        acknowledgeTimeStamp: Date | number;
    };
}
