import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class CreateObject extends BacnetAckService {
    static encode(buffer: EncodeBuffer, objectId: BACNetObjectID, values: any[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        objectId: BACNetObjectID;
        values: any[];
    };
    static encodeAcknowledge(buffer: EncodeBuffer, objectId: BACNetObjectID): void;
    static decodeAcknowledge(buffer: Buffer, offset: number, apduLen: number): any;
}
