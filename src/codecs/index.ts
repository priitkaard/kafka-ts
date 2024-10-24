import { GZIP } from './gzip';
import { NONE } from './none';
import { Codec } from './types';

const codecs: Record<number, Codec> = {
    0: NONE,
    1: GZIP,
};

export const findCodec = (type: number) => {
    const codec = codecs[type];
    if (!codec) {
        throw new Error(`Unsupported codec: ${type}`);
    }
    return codec;
};
