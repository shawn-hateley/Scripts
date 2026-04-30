import { EncodeBuffer, BACNetObjectID, WritePropertyMultipleValue, WritePropertyMultipleObject } from '../types';
import { BacnetService } from './AbstractServices';
export default class WritePropertyMultiple extends BacnetService {
    static encode(buffer: EncodeBuffer, objectId: BACNetObjectID, values: WritePropertyMultipleValue[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        objectId: {
            type: import("../enum").ObjectType;
            instance: number;
        };
        values: any[];
    };
    static encodeObject(buffer: EncodeBuffer, values: WritePropertyMultipleObject[]): void;
}
