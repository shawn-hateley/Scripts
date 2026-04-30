import { BvlcPacket } from './types';
export declare const encode: (buffer: Buffer, func: number, msgLength: number, originatingIP?: string) => number;
export declare const decode: (buffer: Buffer, _offset: number) => BvlcPacket | undefined;
