import { GZIP } from './gzip';
import { NONE } from './none';
import { Codec } from './types';
import { ZSTD } from './zstd';

const codecs: Record<number, Codec> = {
    0: NONE,
    1: GZIP,
    4: ZSTD,
};

export const findCodec = (type: number) => {
    const codec = codecs[type];
    if (!codec) {
        throw new Error(`Unsupported codec: ${type}`);
    }
    return codec;
};
