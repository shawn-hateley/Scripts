import { EncodeBuffer, BACNetObjectID, BACNetEventInformation } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class GetEventInformation extends BacnetAckService {
    static encode(buffer: EncodeBuffer, lastReceivedObjectId: BACNetObjectID): void;
    static decode(buffer: Buffer, offset: number): {
        len: number;
        lastReceivedObjectId: BACNetObjectID;
    };
    static encodeAcknowledge(buffer: EncodeBuffer, events: BACNetEventInformation[], moreEvents: boolean): void;
    static decodeAcknowledge(buffer: Buffer, offset: number, apduLen: number): any;
}
