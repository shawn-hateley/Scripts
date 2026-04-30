"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TypedEventEmitter = void 0;
exports.applyMixin = applyMixin;
const events_1 = require("events");
function applyMixin(target, mixin, includeConstructor = false) {
    const inheritanceChain = [mixin];
    while (true) {
        const current = inheritanceChain[0];
        const base = Object.getPrototypeOf(current);
        if (base?.prototype) {
            inheritanceChain.unshift(base);
        }
        else {
            break;
        }
    }
    for (const ctor of inheritanceChain) {
        for (const prop of Object.getOwnPropertyNames(ctor.prototype)) {
            if (includeConstructor || prop !== 'constructor') {
                Object.defineProperty(target.prototype, prop, Object.getOwnPropertyDescriptor(ctor.prototype, prop) ??
                    Object.create(null));
            }
        }
    }
}
class TypedEventEmitter {
}
exports.TypedEventEmitter = TypedEventEmitter;
applyMixin(TypedEventEmitter, events_1.EventEmitter);
//# sourceMappingURL=EventTypes.js.map