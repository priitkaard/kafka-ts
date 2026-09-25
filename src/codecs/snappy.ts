import { Codec } from './types';

const XERIAL_MAGIC = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]);
const XERIAL_HEADER_SIZE = 16;
const XERIAL_CHUNK_SIZE = 32 * 1024;
const HASH_BITS = 14;

const hash = (input: Buffer, offset: number) => Math.imul(input.readUInt32LE(offset), 0x1e35a7bd) >>> (32 - HASH_BITS);

const writeLiteral = (output: number[], input: Buffer, start: number, end: number) => {
    const length = end - start - 1;
    if (length < 60) {
        output.push(length << 2);
    } else {
        const bytes = length < 0x100 ? 1 : length < 0x10000 ? 2 : length < 0x1000000 ? 3 : 4;
        output.push((59 + bytes) << 2);
        for (let i = 0; i < bytes; i++) output.push((length >> (8 * i)) & 0xff);
    }
    for (let i = start; i < end; i++) output.push(input[i]);
};

const compressBlock = (input: Buffer) => {
    const output: number[] = [];
    for (let length = input.length; ; length >>>= 7) {
        output.push(length < 0x80 ? length : (length & 0x7f) | 0x80);
        if (length < 0x80) break;
    }

    const table = new Int32Array(1 << HASH_BITS).fill(-1);
    let literalStart = 0;
    let offset = 0;
    while (offset + 4 <= input.length) {
        const key = hash(input, offset);
        const candidate = table[key];
        table[key] = offset;
        if (candidate < 0 || input.readUInt32LE(candidate) !== input.readUInt32LE(offset)) {
            offset++;
            continue;
        }

        if (literalStart < offset) writeLiteral(output, input, literalStart, offset);
        let length = 4;
        while (offset + length < input.length && input[candidate + length] === input[offset + length]) length++;

        const distance = offset - candidate;
        for (let remaining = length; remaining > 0;) {
            const copyLength = Math.min(remaining, 64);
            output.push(((copyLength - 1) << 2) | 2, distance & 0xff, distance >> 8);
            remaining -= copyLength;
        }
        offset += length;
        literalStart = offset;
    }
    if (literalStart < input.length) writeLiteral(output, input, literalStart, input.length);
    return Buffer.from(output);
};

const decompressBlock = (input: Buffer) => {
    let offset = 0;
    let length = 0;
    for (let shift = 0; ; shift += 7) {
        const byte = input[offset++];
        length |= (byte & 0x7f) << shift;
        if (!(byte & 0x80)) break;
    }

    const output = Buffer.allocUnsafe(length);
    let outputOffset = 0;
    while (offset < input.length) {
        const tag = input[offset++];
        const type = tag & 0x03;

        if (type === 0) {
            let literalLength = tag >> 2;
            if (literalLength >= 60) {
                const bytes = literalLength - 59;
                literalLength = input.readUIntLE(offset, bytes);
                offset += bytes;
            }
            literalLength += 1;
            input.copy(output, outputOffset, offset, offset + literalLength);
            offset += literalLength;
            outputOffset += literalLength;
            continue;
        }

        let copyLength: number;
        let copyOffset: number;
        if (type === 1) {
            copyLength = ((tag >> 2) & 0x07) + 4;
            copyOffset = ((tag >> 5) << 8) | input[offset++];
        } else if (type === 2) {
            copyLength = (tag >> 2) + 1;
            copyOffset = input.readUInt16LE(offset);
            offset += 2;
        } else {
            copyLength = (tag >> 2) + 1;
            copyOffset = input.readUInt32LE(offset);
            offset += 4;
        }
        for (let i = 0; i < copyLength; i++, outputOffset++) {
            output[outputOffset] = output[outputOffset - copyOffset];
        }
    }
    return output;
};

const compress = (data: Buffer) => {
    const header = Buffer.alloc(XERIAL_HEADER_SIZE);
    XERIAL_MAGIC.copy(header);
    header.writeInt32BE(1, 8);
    header.writeInt32BE(1, 12);

    const chunks = [header];
    for (let offset = 0; offset < data.length; offset += XERIAL_CHUNK_SIZE) {
        const block = compressBlock(data.subarray(offset, offset + XERIAL_CHUNK_SIZE));
        const length = Buffer.alloc(4);
        length.writeInt32BE(block.length);
        chunks.push(length, block);
    }
    return Buffer.concat(chunks);
};

const decompress = (data: Buffer) => {
    if (!data.subarray(0, XERIAL_MAGIC.length).equals(XERIAL_MAGIC)) {
        return decompressBlock(data);
    }

    const chunks: Buffer[] = [];
    for (let offset = XERIAL_HEADER_SIZE; offset < data.length;) {
        const length = data.readInt32BE(offset);
        offset += 4;
        chunks.push(decompressBlock(data.subarray(offset, offset + length)));
        offset += length;
    }
    return Buffer.concat(chunks);
};

export const SNAPPY: Codec = {
    compress: async (data) => compress(data),
    decompress: async (data) => decompress(data),
};
