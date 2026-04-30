import { TypedEventEmitter, TransportEvents } from './EventTypes';
import { TransportSettings } from './types';
export default class Transport extends TypedEventEmitter<TransportEvents> {
    private _settings;
    private _server;
    private _lastSendMessages;
    private ownAddress;
    constructor(settings: TransportSettings);
    getBroadcastAddress(): string;
    getMaxPayload(): number;
    send(buffer: Buffer, offset: number, receiver?: string): void;
    open(): void;
    close(): void;
}
