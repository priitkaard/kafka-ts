import { describe, expect, it } from 'vitest';
import { murmur2, toPositive } from './murmur2';

const vectors: [string, number][] = [
    ['', 275646681],
    ['a', -1563381124],
    ['hello', 2132663229],
    ['user-1234', -1663159204],
    ['order-key-abcdef', 910641134],
    ['x'.repeat(37), -278642013],
    ['kafka-ts', 262808086],
];

describe('murmur2', () => {
    it.each(vectors)('hashes %j the same way Kafka does', (key, expected) => {
        expect(murmur2(Buffer.from(key))).toBe(expected);
    });

    it('stays within the signed 32-bit range', () => {
        vectors.forEach(([key]) => {
            const hash = murmur2(Buffer.from(key));
            expect(Number.isInteger(hash)).toBe(true);
            expect(hash).toBeGreaterThanOrEqual(-(2 ** 31));
            expect(hash).toBeLessThanOrEqual(2 ** 31 - 1);
        });
    });

    it('maps hashes into a positive partition range', () => {
        vectors.forEach(([key]) => {
            expect(toPositive(murmur2(Buffer.from(key))) % 12).toBeGreaterThanOrEqual(0);
        });
    });
});
