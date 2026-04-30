import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetService } from './AbstractServices';
export default class DeleteObject extends BacnetService {
    static encode(buffer: EncodeBuffer, objectId: BACNetObjectID): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): import("../types").ObjectId;
}
