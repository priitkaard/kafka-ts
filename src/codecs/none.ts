import { Codec } from './types';

export const NONE: Codec = {
    compress: (data: Buffer) => Promise.resolve(data),
    decompress: (data: Buffer) => Promise.resolve(data),
};
