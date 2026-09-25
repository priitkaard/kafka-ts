import { Codec } from './types';

const FRAME_MAGIC = 0x184d2204;
const FRAME_HEADER = Buffer.from('04224d18604082', 'hex');
const BLOCK_SIZE = 64 * 1024;
const MIN_MATCH = 4;
const LAST_LITERALS = 5;
const MATCH_FIND_LIMIT = 12;
const HASH_BITS = 14;

const hash = (input: Buffer, offset: number) => Math.imul(input.readUInt32LE(offset), 0x9e3779b1) >>> (32 - HASH_BITS);

const writeLength = (output: number[], length: number) => {
    for (; length >= 255; length -= 255) output.push(255);
    output.push(length);
};

const writeSequence = (output: number[], literals: Buffer, matchOffset = 0, matchLength = 0) => {
    const extraMatchLength = matchLength - MIN_MATCH;
    output.push((Math.min(literals.length, 15) << 4) | (matchLength ? Math.min(extraMatchLength, 15) : 0));
    if (literals.length >= 15) writeLength(output, literals.length - 15);
    for (const byte of literals) output.push(byte);
    if (!matchLength) return;

    output.push(matchOffset & 0xff, matchOffset >> 8);
    if (extraMatchLength >= 15) writeLength(output, extraMatchLength - 15);
};

const compressBlock = (input: Buffer) => {
    const output: number[] = [];
    const table = new Int32Array(1 << HASH_BITS).fill(-1);
    let literalStart = 0;
    let offset = 0;
    while (offset < input.length - MATCH_FIND_LIMIT) {
        const key = hash(input, offset);
        const candidate = table[key];
        table[key] = offset;
        if (candidate < 0 || input.readUInt32LE(candidate) !== input.readUInt32LE(offset)) {
            offset++;
            continue;
        }

        let matchLength = MIN_MATCH;
        while (
            offset + matchLength < input.length - LAST_LITERALS &&
            input[candidate + matchLength] === input[offset + matchLength]
        ) {
            matchLength++;
        }
        writeSequence(output, input.subarray(literalStart, offset), offset - candidate, matchLength);
        offset += matchLength;
        literalStart = offset;
    }
    writeSequence(output, input.subarray(literalStart));
    return Buffer.from(output);
};

const compress = (data: Buffer) => {
    const chunks: Buffer[] = [FRAME_HEADER];
    for (let offset = 0; offset < data.length; offset += BLOCK_SIZE) {
        const block = data.subarray(offset, offset + BLOCK_SIZE);
        const compressed = compressBlock(block);
        const size = Buffer.alloc(4);
        if (compressed.length < block.length) {
            size.writeUInt32LE(compressed.length);
            chunks.push(size, compressed);
        } else {
            size.writeUInt32LE((block.length | 0x80000000) >>> 0);
            chunks.push(size, block);
        }
    }
    chunks.push(Buffer.alloc(4));
    return Buffer.concat(chunks);
};

const decompress = (data: Buffer) => {
    if (data.readUInt32LE(0) !== FRAME_MAGIC) {
        throw new Error('Invalid LZ4 frame');
    }

    const flags = data[4];
    const blockMaxSize = 1 << (2 * ((data[5] >> 4) & 0x07) + 8);
    const hasBlockChecksum = flags & 0x10;
    let offset = 6 + (flags & 0x08 ? 8 : 0) + (flags & 0x01 ? 4 : 0) + 1;

    let output = Buffer.allocUnsafe(0);
    let outputOffset = 0;
    const reserve = (size: number) => {
        if (outputOffset + size <= output.length) return;
        const grown = Buffer.allocUnsafe(Math.max(output.length * 2, outputOffset + size));
        output.copy(grown, 0, 0, outputOffset);
        output = grown;
    };

    while (true) {
        const blockSize = data.readUInt32LE(offset);
        offset += 4;
        if (!blockSize) break;

        const size = blockSize & 0x7fffffff;
        const block = data.subarray(offset, offset + size);
        offset += size + (hasBlockChecksum ? 4 : 0);

        reserve(blockMaxSize);
        if (blockSize & 0x80000000) {
            block.copy(output, outputOffset);
            outputOffset += size;
            continue;
        }

        for (let blockOffset = 0; blockOffset < block.length;) {
            const token = block[blockOffset++];

            let literalLength = token >> 4;
            if (literalLength === 15) {
                for (let byte = 255; byte === 255; literalLength += byte) byte = block[blockOffset++];
            }
            block.copy(output, outputOffset, blockOffset, blockOffset + literalLength);
            blockOffset += literalLength;
            outputOffset += literalLength;
            if (blockOffset >= block.length) break;

            const matchOffset = block.readUInt16LE(blockOffset);
            blockOffset += 2;
            let matchLength = token & 0x0f;
            if (matchLength === 15) {
                for (let byte = 255; byte === 255; matchLength += byte) byte = block[blockOffset++];
            }
            matchLength += 4;
            for (let i = 0; i < matchLength; i++, outputOffset++) {
                output[outputOffset] = output[outputOffset - matchOffset];
            }
        }
    }
    return output.subarray(0, outputOffset);
};

export const LZ4: Codec = {
    compress: async (data) => compress(data),
    decompress: async (data) => decompress(data),
};
