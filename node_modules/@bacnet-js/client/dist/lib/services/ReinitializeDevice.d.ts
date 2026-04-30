import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class ReinitializeDevice extends BacnetService {
    static encode(buffer: EncodeBuffer, state: number, password: string): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
