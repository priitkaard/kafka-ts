import { describe, expect, it } from 'vitest';
import { gzipSync, zstdCompressSync } from 'zlib';
import { Decoder } from '../../utils/decoder';
import { createBatch } from '../produce/common';
import { decodeRecordBatch, withDecompressions } from './common';

const RECORDS_OFFSET = 61;

const createCompressedBatch = (compression: number, compress: (data: Buffer) => Buffer) => {
    const batch = createBatch({
        index: 0,
        baseOffset: 0n,
        partitionLeaderEpoch: 0,
        attributes: compression,
        lastOffsetDelta: 1,
        baseTimestamp: 0n,
        maxTimestamp: 0n,
        producerId: -1n,
        producerEpoch: -1,
        baseSequence: -1,
        records: ['a', 'b'].map((value, offsetDelta) => ({
            attributes: 0,
            timestampDelta: 0n,
            offsetDelta,
            key: null,
            value,
            headers: [],
        })),
    }).value();

    const compressed = Buffer.concat([batch.subarray(0, RECORDS_OFFSET), compress(batch.subarray(RECORDS_OFFSET))]);
    compressed.writeInt32BE(compressed.length - 12, 8);
    return compressed;
};

describe('decodeRecordBatch', () => {
    it.each([
        ['gzip', 1, gzipSync],
        ['zstd', 4, zstdCompressSync],
    ])('decodes %s compressed records', async (_, compression, compress) => {
        const batch = createCompressedBatch(compression, compress);

        const [{ records }] = await withDecompressions((decompressions) =>
            decodeRecordBatch(new Decoder(batch), batch.length, decompressions),
        );

        expect(records.map(({ value }) => value)).toEqual(['a', 'b']);
    });
});
