import ModbusAbstractRequest from './abstract-request';
import ModbusAbstractResponse from './abstract-response';
export declare type UserRequestErrorCodes = 'OutOfSync' | 'Protocol' | 'Timeout' | 'ManuallyCleared' | 'ModbusException' | 'Offline' | 'crcMismatch';
export interface IUserRequestError<Res extends ModbusAbstractResponse, Req extends ModbusAbstractRequest> {
    err: UserRequestErrorCodes;
    message: string;
    response?: Res;
    request?: Req;
}
export declare class UserRequestError<Res extends ModbusAbstractResponse, Req extends ModbusAbstractRequest> implements IUserRequestError<Res, Req> {
    err: UserRequestErrorCodes;
    message: string;
    request?: Req;
    response?: Res;
    constructor({ err, message, response, request }: IUserRequestError<Res, Req>);
}
export declare function isUserRequestError(x: any): x is UserRequestError<any, any>;
//# sourceMappingURL=user-request-error.d.ts.map