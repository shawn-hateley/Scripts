import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class IAm extends BacnetService {
    static encode(buffer: EncodeBuffer, deviceId: number, maxApdu: number, segmentation: number, vendorId: number): void;
    static decode(buffer: Buffer, offset: number): {
        len: number;
        deviceId: any;
        maxApdu: any;
        segmentation: any;
        vendorId: any;
    };
}
