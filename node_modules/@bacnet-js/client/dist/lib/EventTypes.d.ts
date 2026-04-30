import { BACnetMessage, BACnetMessageHeader, IAMResult, WhoIsResult, BACNetEventInformation, BACNetAlarm, ServiceMessage, SimpleAckPayload, CovNotifyPayload, AtomicFilePayload, SubscribeCovPayload, DeviceCommunicationControlPayload, ReinitializeDevicePayload, EventNotificationPayload, ReadRangePayload, ObjectOperationPayload, ListElementOperationPayload, PrivateTransferPayload, RegisterForeignDevicePayload, BvlcResultPayload, WhoHasPayload, TimeSyncPayload, IHavePayload, BACNetObjectID, BACNetPropertyID, BACNetAppData } from './types';
export type Constructor<T = object> = new (...args: any[]) => T;
export declare function applyMixin(target: Constructor, mixin: Constructor, includeConstructor?: boolean): void;
export interface BaseEventContent {
    header?: BACnetMessageHeader;
    payload: any;
    service?: number;
    invokeId?: number;
}
export interface ReadPropertyContent extends BaseEventContent {
    payload: {
        objectId: BACNetObjectID;
        property: BACNetPropertyID;
    };
    address?: string;
}
export interface WritePropertyContent extends BaseEventContent {
    payload: {
        objectId: BACNetObjectID;
        property?: BACNetPropertyID;
        value?: {
            property?: BACNetPropertyID;
            value?: BACNetAppData | BACNetAppData[];
        };
    };
}
export interface ReadPropertyMultipleContent extends BaseEventContent {
    payload: {
        properties: Array<{
            objectId: BACNetObjectID;
            properties: Array<{
                id: number;
                index: number;
            }>;
        }>;
    };
}
export interface SubscribeCovContent extends BaseEventContent {
    payload: SubscribeCovPayload;
}
export interface BACnetClientEvents {
    message: (message: BACnetMessage, rinfo: string) => void;
    error: (error: Error) => void;
    listening: () => void;
    unhandledEvent: (content: ServiceMessage) => void;
    readProperty: (content: ReadPropertyContent) => void;
    writeProperty: (content: WritePropertyContent) => void;
    readPropertyMultiple: (content: ReadPropertyMultipleContent) => void;
    writePropertyMultiple: (content: BaseEventContent & {
        payload: {
            objectId: BACNetObjectID;
            values: any[];
        };
    }) => void;
    covNotify: (content: BaseEventContent & {
        payload: CovNotifyPayload;
    }) => void;
    atomicWriteFile: (content: BaseEventContent & {
        payload: AtomicFilePayload;
    }) => void;
    atomicReadFile: (content: BaseEventContent & {
        payload: AtomicFilePayload;
    }) => void;
    subscribeCov: (content: SubscribeCovContent) => void;
    subscribeProperty: (content: BaseEventContent & {
        payload: SubscribeCovPayload;
    }) => void;
    deviceCommunicationControl: (content: BaseEventContent & {
        payload: DeviceCommunicationControlPayload;
    }) => void;
    reinitializeDevice: (content: BaseEventContent & {
        payload: ReinitializeDevicePayload;
    }) => void;
    eventNotify: (content: BaseEventContent & {
        payload: EventNotificationPayload;
    }) => void;
    readRange: (content: BaseEventContent & {
        payload: ReadRangePayload;
    }) => void;
    createObject: (content: BaseEventContent & {
        payload: ObjectOperationPayload;
    }) => void;
    deleteObject: (content: BaseEventContent & {
        payload: ObjectOperationPayload;
    }) => void;
    alarmAcknowledge: (content: BaseEventContent & {
        payload: SimpleAckPayload;
    }) => void;
    getAlarmSummary: (content: BaseEventContent & {
        payload: BACNetAlarm[];
    }) => void;
    getEnrollmentSummary: (content: BaseEventContent & {
        payload: any;
    }) => void;
    getEventInformation: (content: BaseEventContent & {
        payload: BACNetEventInformation[];
    }) => void;
    lifeSafetyOperation: (content: BaseEventContent & {
        payload: any;
    }) => void;
    addListElement: (content: BaseEventContent & {
        payload: ListElementOperationPayload;
    }) => void;
    removeListElement: (content: BaseEventContent & {
        payload: ListElementOperationPayload;
    }) => void;
    privateTransfer: (content: BaseEventContent & {
        payload: PrivateTransferPayload;
    }) => void;
    bvlcResult: (content: BaseEventContent & {
        payload: BvlcResultPayload;
    }) => void;
    registerForeignDevice: (content: BaseEventContent & {
        payload: RegisterForeignDevicePayload;
    }) => void;
    iAm: (content: BaseEventContent & {
        payload: IAMResult;
    }) => void;
    whoIs: (content: BaseEventContent & {
        payload: WhoIsResult;
    }) => void;
    whoHas: (content: BaseEventContent & {
        payload: WhoHasPayload;
    }) => void;
    covNotifyUnconfirmed: (content: BaseEventContent & {
        payload: CovNotifyPayload;
    }) => void;
    timeSync: (content: BaseEventContent & {
        payload: TimeSyncPayload;
    }) => void;
    timeSyncUTC: (content: BaseEventContent & {
        payload: TimeSyncPayload;
    }) => void;
    iHave: (content: BaseEventContent & {
        payload: IHavePayload;
    }) => void;
}
export type BACnetEventsMap = {
    [key: number]: keyof BACnetClientEvents;
};
export interface TransportEvents {
    message: (buffer: Buffer, remoteAddress: string) => void;
    listening: (address: {
        address: string;
        port: number;
    }) => void;
    error: (error: Error) => void;
    close: () => void;
}
export type EventHandler = ((arg1: any, arg2: any, arg3: any, arg4: any) => void) | ((arg1: any, arg2: any, arg3: any) => void) | ((arg1: any, arg2: any) => void) | ((arg1: any) => void) | ((...args: any[]) => void);
export type THandler<TEvents> = Record<keyof TEvents, EventHandler>;
export interface TypedEventEmitter<TEvents extends Record<keyof TEvents, EventHandler>> {
    on<TEvent extends keyof TEvents>(event: TEvent, callback: TEvents[TEvent]): this;
    once<TEvent extends keyof TEvents>(event: TEvent, callback: TEvents[TEvent]): this;
    prependListener<TEvent extends keyof TEvents>(event: TEvent, callback: TEvents[TEvent]): this;
    prependOnceListener<TEvent extends keyof TEvents>(event: TEvent, callback: TEvents[TEvent]): this;
    removeListener<TEvent extends keyof TEvents>(event: TEvent, callback: TEvents[TEvent]): this;
    off<TEvent extends keyof TEvents>(event: TEvent, callback: TEvents[TEvent]): this;
    removeAllListeners(event?: keyof TEvents): this;
    emit<TEvent extends keyof TEvents>(event: TEvent, ...args: Parameters<TEvents[TEvent]>): boolean;
    setMaxListeners(n: number): this;
    getMaxListeners(): number;
    listeners<TEvent extends keyof TEvents>(eventName: TEvent): TEvents[TEvent][];
    rawListeners<TEvent extends keyof TEvents>(eventName: TEvent): TEvents[TEvent][];
    listenerCount<TEvent extends keyof TEvents>(event: TEvent, listener?: TEvents[TEvent]): number;
    eventNames(): Array<keyof TEvents>;
}
export declare class TypedEventEmitter<TEvents extends THandler<TEvents>> {
}
