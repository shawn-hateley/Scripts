import { type EnumType, type StatusFlags, type ObjectTypesSupported, type ServicesSupported } from './enum';
import { type BACNetBitString } from './types';
export declare const MAX_BITSTRING_BITS: number;
export declare class GenericBitString<E extends EnumType<E>> implements BACNetBitString {
    readonly bitsUsed: number;
    readonly value: number[];
    constructor(bitsUsed: number, trueBits: E[keyof E][]);
}
export declare class StatusFlagsBitString extends GenericBitString<typeof StatusFlags> {
    constructor(...trueBits: StatusFlags[]);
}
export declare class ObjectTypesSupportedBitString extends GenericBitString<typeof ObjectTypesSupported> {
    constructor(...trueBits: ObjectTypesSupported[]);
}
export declare class ServicesSupportedBitString extends GenericBitString<typeof ServicesSupported> {
    constructor(...trueBits: ServicesSupported[]);
}
