import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetService } from './AbstractServices';
export default class CovNotify extends BacnetService {
    static encode(buffer: EncodeBuffer, subscriberProcessId: number, initiatingDeviceId: number, monitoredObjectId: BACNetObjectID, timeRemaining: number, values: any[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        subscriberProcessId: any;
        initiatingDeviceId: {
            type: any;
            instance: any;
        };
        monitoredObjectId: {
            type: any;
            instance: any;
        };
        timeRemaining: any;
        values: any[];
    };
}
