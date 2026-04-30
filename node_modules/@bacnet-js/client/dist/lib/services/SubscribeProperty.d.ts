import { EncodeBuffer, BACNetObjectID, BACNetPropertyID } from '../types';
import { BacnetService } from './AbstractServices';
export default class SubscribeProperty extends BacnetService {
    static encode(buffer: EncodeBuffer, subscriberProcessId: number, monitoredObjectId: BACNetObjectID, cancellationRequest: boolean, issueConfirmedNotifications: boolean, lifetime: number, monitoredProperty: BACNetPropertyID, covIncrementPresent: boolean, covIncrement: number): void;
    static decode(buffer: Buffer, offset: number): any;
}
