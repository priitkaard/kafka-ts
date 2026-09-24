import { describe, expect, it, vi } from 'vitest';
import { shared } from './shared';

describe('shared', () => {
    it('deduplicates concurrent calls with the same arguments', async () => {
        const func = vi.fn(async (value: number) => value);
        const sharedFunc = shared(func);

        await Promise.all([sharedFunc(1), sharedFunc(1), sharedFunc(2)]);

        expect(func).toHaveBeenCalledTimes(2);
    });

    it('surfaces a rejection only to the caller', async () => {
        const sharedFunc = shared(async () => {
            throw new Error('boom');
        });

        await expect(sharedFunc()).rejects.toThrow('boom');
        await expect(sharedFunc()).rejects.toThrow('boom');
    });
});
