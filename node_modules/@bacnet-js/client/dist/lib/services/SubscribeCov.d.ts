import { EncodeBuffer, BACNetObjectID } from '../types';
import { BacnetService } from './AbstractServices';
export default class SubscribeCov extends BacnetService {
    static encode(buffer: EncodeBuffer, subscriberProcessId: number, monitoredObjectId: BACNetObjectID, cancellationRequest: boolean, issueConfirmedNotifications?: boolean, lifetime?: number): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): any;
}
