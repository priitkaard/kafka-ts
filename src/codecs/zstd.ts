import { zstdCompress, zstdDecompress } from 'zlib';
import { Codec } from './types';

export const ZSTD: Codec = {
    compress: async (data) =>
        new Promise<Buffer>((resolve, reject) =>
            zstdCompress(data, (err, result) => (err ? reject(err) : resolve(result))),
        ),
    decompress: async (data) =>
        new Promise<Buffer>((resolve, reject) =>
            zstdDecompress(data, (err, result) => (err ? reject(err) : resolve(result))),
        ),
};
