import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class PrivateTransfer extends BacnetService {
    static encode(buffer: EncodeBuffer, vendorId: number, serviceNumber: number, data: number[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
