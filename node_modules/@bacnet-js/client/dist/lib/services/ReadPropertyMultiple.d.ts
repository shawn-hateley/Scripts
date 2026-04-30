import { BACNetReadAccessSpecification, DecodeAcknowledgeMultipleResult, EncodeBuffer } from '../types';
import { BacnetAckService } from './AbstractServices';
export default class ReadPropertyMultiple extends BacnetAckService {
    static encode(buffer: EncodeBuffer, properties: BACNetReadAccessSpecification[]): void;
    static decode(buffer: Buffer, offset: number, apduLen: number): {
        len: number;
        properties: BACNetReadAccessSpecification[];
    };
    static encodeAcknowledge(buffer: EncodeBuffer, values: any[]): void;
    static decodeAcknowledge(buffer: Buffer, offset: number, apduLen: number): DecodeAcknowledgeMultipleResult;
}
