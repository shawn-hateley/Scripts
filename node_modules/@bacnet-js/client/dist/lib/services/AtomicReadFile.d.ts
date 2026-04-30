import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class AtomicReadFile extends BacnetAckService {
    static encode(buffer: EncodeBuffer, isStream: boolean, objectId: BACNetObjectID, position: number, count: number): void;
    static decode(buffer: Buffer, offset: number): {
        len: number;
        isStream: boolean;
        objectId: BACNetObjectID;
        position: number;
        count: number;
    };
    static encodeAcknowledge(buffer: EncodeBuffer, isStream: boolean, endOfFile: boolean, position: number, blockCount: number, blocks: number[][], counts: number[]): void;
    static decodeAcknowledge(buffer: Buffer, offset: number): {
        len: number;
        endOfFile: boolean;
        isStream: boolean;
        position: number;
        buffer: Buffer<ArrayBufferLike>;
    };
}
