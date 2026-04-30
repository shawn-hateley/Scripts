import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetService } from './AbstractServices';
export default class LifeSafetyOperation extends BacnetService {
    static encode(buffer: EncodeBuffer, processId: number, requestingSource: string, operation: number, targetObjectId: BACNetObjectID): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
