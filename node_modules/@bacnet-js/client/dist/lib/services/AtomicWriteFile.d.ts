import { BACNetObjectID, EncodeBuffer } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class AtomicWriteFile extends BacnetAckService {
    static encode(buffer: EncodeBuffer, isStream: boolean, objectId: BACNetObjectID, position: number, blocks: number[][]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        isStream: boolean;
        objectId: BACNetObjectID;
        position: number;
        blocks: any[];
    };
    static encodeAcknowledge(buffer: EncodeBuffer, isStream: boolean, position: number): void;
    static decodeAcknowledge(buffer: Buffer, offset: number): {
        len: number;
        isStream: boolean;
        position: number;
    };
}
