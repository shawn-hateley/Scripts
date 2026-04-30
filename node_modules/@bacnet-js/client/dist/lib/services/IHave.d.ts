import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetService } from './AbstractServices';
export default class IHave extends BacnetService {
    static encode(buffer: EncodeBuffer, deviceId: BACNetObjectID, objectId: BACNetObjectID, objectName: string): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
