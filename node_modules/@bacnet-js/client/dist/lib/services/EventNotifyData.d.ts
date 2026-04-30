import { EncodeBuffer, EventNotifyDataParams, EventNotifyDataResult } from '../types';
import { BacnetService } from './AbstractServices';
export default class EventNotifyData extends BacnetService {
    static encode(buffer: EncodeBuffer, data: EventNotifyDataParams): void;
    static decode(buffer: Buffer, offset: number): EventNotifyDataResult | undefined;
}
