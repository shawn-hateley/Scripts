import { BACNetObjectID, EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class WhoHas extends BacnetService {
    static encode(buffer: EncodeBuffer, lowLimit: number, highLimit: number, objectId: BACNetObjectID, objectName?: string): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
