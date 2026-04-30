import { EncodeBuffer, DecodedNpdu, BACNetAddress } from './types';
export declare const decodeFunction: (buffer: Buffer, offset: number) => number | undefined;
export declare const decode: (buffer: Buffer, offset: number) => DecodedNpdu | undefined;
export declare const encode: (buffer: EncodeBuffer, funct: number, destination?: BACNetAddress, source?: BACNetAddress, hopCount?: number, networkMsgType?: number, vendorId?: number) => void;
