import { EncodeBuffer, BACNetObjectID, BACNetAppData } from '../types';
import { BacnetService } from './AbstractServices';
export default class AddListElement extends BacnetService {
    static encode(buffer: EncodeBuffer, objectId: BACNetObjectID, propertyId: number, arrayIndex: number, values: BACNetAppData[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        objectId: BACNetObjectID;
        property: {
            id: number;
            index: number;
        };
        values: BACNetAppData[];
    };
}
