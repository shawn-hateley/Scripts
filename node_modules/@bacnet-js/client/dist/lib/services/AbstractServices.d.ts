import { EncodeBuffer } from '../types';
export declare abstract class BacnetService {
    static encode(buffer: EncodeBuffer, ...args: any[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
export declare abstract class BacnetAckService extends BacnetService {
    static encodeAcknowledge(buffer: EncodeBuffer, ...args: any[]): void;
    static decodeAcknowledge(buffer: Buffer, offset: number, apduLen: number): any;
}
