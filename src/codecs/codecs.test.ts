import { randomBytes } from 'crypto';
import { describe, expect, it } from 'vitest';
import { LZ4 } from './lz4';
import { SNAPPY } from './snappy';

const input = Buffer.from(
    Array.from({ length: 40 }, (_, i) => `record-${i % 7}-${'x'.repeat(i % 13)}`).join(',') +
        Array.from({ length: 40 }, (_, i) => (i * 7919) % 1000).join(''),
);
const random = Buffer.from('3f9a0c71e25b84d6a1c93e07f5286bd41e9c3a70528fb6d9e10473ac5d28b9f6', 'hex');

describe('codecs', () => {
    it.each([
        [
            'snappy written by the Java client',
            SNAPPY,
            'glNOQVBQWQAAAAABAAAAAQAAAUTqBSRyZWNvcmQtMC0sDQoIMS14EQsIMi14FQwIMy14GQ0ANAENFRsANQUOFQ8ANgkPFRAAMA0QFREAMRERFRIBaQ0BEXwFcA0BERQJdw0BERUNfg0BERYANhXQADAZ0AF5ESEFcxENCW0RDg1nEQ8RYREQEeAVERXhFRIBaQ0BETYFcA0BERQJdw0BERUNfg0BERYANRXQADYZ0AF5ESEFcxENCW0RDg1nEQ8RYREQEeAVERXhFRIBaQ0BETYFcA0BERQJdw0BERUNfg0BERbwcjQtMDkxOTgzODc1NzY3NjU5NTUxNDQzMzM1MjI3MTE5MDEwOTI4OTQ3ODY2Nzg1NzA0NjIzNTQyNDYxMzgwMjk5MjE4MTM3NTY5NzU4OTQ4MTM3MzI2NTE1NzA0ODk0MDgzMjcyNDYxNjU4NDM5MjI4NDE=',
            input,
        ],
        [
            'raw snappy',
            SNAPPY,
            '6gUkcmVjb3JkLTAtLA0KCDEteBELCDIteBUMCDMteBkNADQBDRUbADUFDhUPADYJDxUQADANEBURADERERUSAWkNARF8BXANAREUCXcNAREVDX4NAREWADYV0AAwGdABeREhBXMRDQltEQ4NZxEPEWEREBHgFREV4RUSAWkNARE2BXANAREUCXcNAREVDX4NAREWADUV0AA2GdABeREhBXMRDQltEQ4NZxEPEWEREBHgFREV4RUSAWkNARE2BXANAREUCXcNAREVDX4NAREW8HI0LTA5MTk4Mzg3NTc2NzY1OTU1MTQ0MzMzNTIyNzExOTAxMDkyODk0Nzg2Njc4NTcwNDYyMzU0MjQ2MTM4MDI5OTIxODEzNzU2OTc1ODk0ODEzNzMyNjUxNTcwNDg5NDA4MzI3MjQ2MTY1ODQzOTIyODQx',
            input,
        ],
        [
            'lz4 written by the Java client',
            LZ4,
            'BCJNGGBAgoUBAACjcmVjb3JkLTAtLAoANDEteAsANTIteAwANjMteA0AEDQNAAUbABE1DgAFDwASNg8ABRAAEzAQAAURABQxEQAFEgAAaQADAgAEfAABcAADAgAEFAACdwADAgAEFQADfgADAgAEFgAVNtAAFjDQAAB5AAQhAAFzAAQNAAJtAAQOAANnAAQPAARhAAQQAATgAAW+AAXhAAUSAABpAAMCAAQ2AAFwAAMCAAQUAAJ3AAMCAAQVAAN+AAMCAAQWABU10AAWNtAAAHkABCEAAXMABA0AAm0ABA4AA2cABA8ABGEABBAABOAABb4ABeEABRIAAGkAAwIABDYAAXAAAwIABBQAAncAAwIABBUAA34AAwIABBYA8Ek0LTA5MTk4Mzg3NTc2NzY1OTU1MTQ0MzMzNTIyNzExOTAxMDkyODk0Nzg2Njc4NTcwNDYyMzU0MjQ2MTM4MDI5OTIxODEzNzU2OTc1ODk0ODEzNzMyNjUxKgCAODk0MDgzMjctALA2NTg0MzkyMjg0MQAAAAA=',
            input,
        ],
        [
            'lz4 with linked blocks and checksums',
            LZ4,
            'BCJNGHxA6gIAAAAAAAC+hQEAAKNyZWNvcmQtMC0sCgA0MS14CwA1Mi14DAA2My14DQAQNA0ABRsAETUOAAUPABI2DwAFEAATMBAABREAFDERAAUSAABpAAMCAAR8AAFwAAMCAAQUAAJ3AAMCAAQVAAN+AAMCAAQWABU20AAWMNAAAHkABCEAAXMABA0AAm0ABA4AA2cABA8ABGEABBAABOAABb4ABeEABRIAAGkAAwIABDYAAXAAAwIABBQAAncAAwIABBUAA34AAwIABBYAFTXQABY20AAAeQAEIQABcwAEDQACbQAEDgADZwAEDwAEYQAEEAAE4AAFvgAF4QAFEgAAaQADAgAENgABcAADAgAEFAACdwADAgAEFQADfgADAgAEFgDwSTQtMDkxOTgzODc1NzY3NjU5NTUxNDQzMzM1MjI3MTE5MDEwOTI4OTQ3ODY2Nzg1NzA0NjIzNTQyNDYxMzgwMjk5MjE4MTM3NTY5NzU4OTQ4MTM3MzI2NTEqAIA4OTQwODMyNy0AsDY1ODQzOTIyODQxLTOzgAAAAABVk2hW',
            input,
        ],
        [
            'lz4 with an uncompressed block',
            LZ4,
            'BCJNGGRApyAAAIA/mgxx4luE1qHJPgf1KGvUHpw6cFKPttnhBHOsXSi59gAAAAC89d+w',
            random,
        ],
    ])('decompresses %s', async (_, codec, compressed, expected) => {
        expect(await codec.decompress(Buffer.from(compressed, 'base64'))).toEqual(expected);
    });

    it.each([
        ['empty', Buffer.alloc(0)],
        ['tiny', Buffer.from('abc')],
        ['random', randomBytes(100_000)],
        ['repetitive', Buffer.from('kafka-ts '.repeat(50_000))],
        ['mixed', Buffer.concat(Array.from({ length: 50 }, (_, i) => Buffer.concat([input, randomBytes(i * 100)])))],
    ])('round-trips %s data', async (_, data) => {
        for (const codec of [SNAPPY, LZ4]) {
            expect(await codec.decompress(await codec.compress!(data))).toEqual(data);
        }
    });
});
