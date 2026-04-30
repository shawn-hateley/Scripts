"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ServicesSupportedBitString = exports.ObjectTypesSupportedBitString = exports.StatusFlagsBitString = exports.GenericBitString = exports.MAX_BITSTRING_BITS = void 0;
const enum_1 = require("./enum");
exports.MAX_BITSTRING_BITS = enum_1.ASN1_MAX_BITSTRING_BYTES * 8;
class GenericBitString {
    bitsUsed;
    value;
    constructor(bitsUsed, trueBits) {
        if (bitsUsed > exports.MAX_BITSTRING_BITS) {
            throw new Error(`Bitstring too large; a bitstring cannot exceed ${exports.MAX_BITSTRING_BITS}`);
        }
        this.bitsUsed = bitsUsed;
        this.value = new Array(Math.ceil(bitsUsed / 8)).fill(0);
        for (const bitIndex of trueBits) {
            if (typeof bitIndex === 'number') {
                if (bitIndex >= bitsUsed) {
                    throw new Error(`Bit index ${bitIndex} is out of range for a bitstring of length ${bitsUsed}`);
                }
                this.value[Math.floor(bitIndex / 8)] |= 1 << bitIndex % 8;
            }
        }
    }
}
exports.GenericBitString = GenericBitString;
class StatusFlagsBitString extends GenericBitString {
    constructor(...trueBits) {
        super(4, trueBits);
    }
}
exports.StatusFlagsBitString = StatusFlagsBitString;
class ObjectTypesSupportedBitString extends GenericBitString {
    constructor(...trueBits) {
        super(80, trueBits);
    }
}
exports.ObjectTypesSupportedBitString = ObjectTypesSupportedBitString;
class ServicesSupportedBitString extends GenericBitString {
    constructor(...trueBits) {
        super(40, trueBits);
    }
}
exports.ServicesSupportedBitString = ServicesSupportedBitString;
//# sourceMappingURL=bitstring.js.map