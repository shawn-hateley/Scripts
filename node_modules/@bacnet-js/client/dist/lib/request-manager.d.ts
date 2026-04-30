import { type NetworkOpResult } from './types';
export declare class RequestManager {
    #private;
    constructor(delay: number, _setTimeout?: typeof setTimeout);
    add(invokeId: number): Promise<NetworkOpResult>;
    resolve(invokeId: number, err: Error, result?: undefined): boolean;
    resolve(invokeId: number, err: null | undefined, result: NetworkOpResult): boolean;
    clear: (force?: boolean) => void;
}
