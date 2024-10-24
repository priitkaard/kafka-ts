import { gzip, unzip } from 'zlib';
import { Codec } from './types';

export const GZIP: Codec = {
    compress: async (data) =>
        new Promise<Buffer>((resolve, reject) => gzip(data, (err, result) => (err ? reject(err) : resolve(result)))),
    decompress: async (data) =>
        new Promise<Buffer>((resolve, reject) => unzip(data, (err, result) => (err ? reject(err) : resolve(result)))),
};
