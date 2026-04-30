import { EncodeBuffer, BACNetEvent } from '../types';
import { BacnetService } from './AbstractServices';
export default class EventInformation extends BacnetService {
    static encode(buffer: EncodeBuffer, events: BACNetEvent[], moreEvents: boolean): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        alarms: any[];
        moreEvents: boolean;
    };
}
