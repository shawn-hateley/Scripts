import { EncodeBuffer, BACNetObjectID, BACNetBitString, ReadRangeAcknowledge } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class ReadRange extends BacnetAckService {
    static encode(buffer: EncodeBuffer, objectId: BACNetObjectID, propertyId: number, arrayIndex: number, requestType: number, position: number, time: Date, count: number): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        objectId: {
            type: any;
            instance: any;
        };
        property: any;
        requestType: number;
        position: number;
        time: Date;
        count: number;
    };
    static encodeAcknowledge(buffer: EncodeBuffer, objectId: BACNetObjectID, propertyId: number, arrayIndex: number, resultFlags: BACNetBitString, itemCount: number, applicationData: Buffer, requestType: number, firstSequence: number): void;
    static decodeAcknowledge(buffer: Buffer, offset: number, apduLen: number): ReadRangeAcknowledge;
}
