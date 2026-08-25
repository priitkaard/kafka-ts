import { describe, expect, it } from 'vitest';
import { Encoder } from './encoder';

describe('Encoder', () => {
    it('grows past the 32-bit doubling boundary without hanging', () => {
        const encoder = new Encoder();
        const chunk = Buffer.alloc(1024);

        for (let i = 0; i < 2048; i++) encoder.write(chunk);

        expect(encoder.getBufferLength()).toBe(2048 * 1024);
    });

    it('throws instead of hanging when a request exceeds the maximum buffer size', () => {
        const encoder = new Encoder();

        expect(() => encoder.write(Buffer.alloc(8))).not.toThrow();
        expect(() => (encoder as any).ensure(Number.MAX_SAFE_INTEGER)).toThrow();
    });
});
