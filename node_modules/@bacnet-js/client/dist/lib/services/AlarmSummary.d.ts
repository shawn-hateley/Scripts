import { EncodeBuffer, BACNetAlarm } from '../types';
import { BacnetService } from './AbstractServices';
export default class AlarmSummary extends BacnetService {
    static encode(buffer: EncodeBuffer, alarms: BACNetAlarm[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        alarms: BACNetAlarm[];
    };
}
