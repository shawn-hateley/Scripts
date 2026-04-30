"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BacnetAckService = exports.BacnetService = void 0;
class BacnetService {
    static encode(buffer, ...args) {
        throw new Error('Method must be implemented by derived class');
    }
    static decode(buffer, offset, apduLen) {
        throw new Error('Method must be implemented by derived class');
    }
}
exports.BacnetService = BacnetService;
class BacnetAckService extends BacnetService {
    static encodeAcknowledge(buffer, ...args) {
        throw new Error('Method must be implemented by derived class');
    }
    static decodeAcknowledge(buffer, offset, apduLen) {
        throw new Error('Method must be implemented by derived class');
    }
}
exports.BacnetAckService = BacnetAckService;
//# sourceMappingURL=AbstractServices.js.map