import { GZIP } from './gzip';
import { LZ4 } from './lz4';
import { NONE } from './none';
import { SNAPPY } from './snappy';
import { Codec } from './types';
import { ZSTD } from './zstd';

const codecs: Record<number, Codec> = {
    0: NONE,
    1: GZIP,
    2: SNAPPY,
    3: LZ4,
    4: ZSTD,
};

export const findCodec = (type: number) => {
    const codec = codecs[type];
    if (!codec) {
        throw new Error(`Unsupported codec: ${type}`);
    }
    return codec;
};
