import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class WhoIs extends BacnetService {
    static encode(buffer: EncodeBuffer, lowLimit: number, highLimit: number): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
