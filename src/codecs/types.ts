export type Codec = {
    compress: (data: Buffer) => Promise<Buffer>;
    decompress: (data: Buffer) => Promise<Buffer>;
};
