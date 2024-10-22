import { createHash, createHmac, pbkdf2, randomBytes } from 'crypto';

export const generateNonce = () => randomBytes(16).toString('base64').replace(/[\/=]/g, '');

export const saltPassword = (password: string, salt: string, iterations: number, keyLength: number, digest: string) =>
    new Promise<Buffer>((resolve, reject) =>
        pbkdf2(password, salt, iterations, keyLength, digest, (err, key) => (err ? reject(err) : resolve(key))),
    );

export const base64Encode = (input: Buffer | string) => Buffer.from(input).toString('base64');
export const base64Decode = (input: string) => Buffer.from(input, 'base64').toString();
export const hash = (data: Buffer) => createHash('sha256').update(data).digest();
export const hmac = (key: Buffer, data: Buffer | string) => createHmac('sha256', key).update(data).digest();
export const xor = (a: Buffer, b: Buffer) => Buffer.from(a.map((byte, i) => byte ^ b[i]));
