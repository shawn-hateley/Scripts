import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class ErrorService extends BacnetService {
    static encode(buffer: EncodeBuffer, errorClass: number, errorCode: number): void;
    static decode(buffer: Buffer, offset: number): {
        len: number;
        class: number;
        code: number;
    };
    static buildMessage(result: {
        class: number;
        code: number;
    }): string;
}
