import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class TimeSync extends BacnetService {
    static encode(buffer: EncodeBuffer, time: Date): void;
    static decode(buffer: Buffer, offset: number): {
        len: number;
        value: Date;
    };
}
