import { BacnetService } from './AbstractServices';
interface DecodeResult {
    len: number;
    ttl: number;
}
export default class RegisterForeignDevice extends BacnetService {
    static encode(buffer: {
        buffer: Buffer;
        offset: number;
    }, ttl: number, length?: number): void;
    static decode(buffer: Buffer, offset: number, length?: number): DecodeResult;
}
export {};
