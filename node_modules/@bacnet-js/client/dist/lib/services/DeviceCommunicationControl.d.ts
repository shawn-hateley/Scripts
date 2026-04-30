import { EncodeBuffer } from '../types';
import { BacnetService } from './AbstractServices';
export default class DeviceCommunicationControl extends BacnetService {
    static encode(buffer: EncodeBuffer, timeDuration: number, enableDisable: number, password: string): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
